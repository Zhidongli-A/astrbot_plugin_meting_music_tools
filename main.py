import asyncio
import hashlib
import json
import mimetypes
import os
import re
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
import imageio_ffmpeg as ffmpeg
import machineid
from packaging.version import parse as parse_version

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import File, Json, Record
from astrbot.api.star import Context, Star, register
from astrbot.core.config.default import VERSION
from astrbot.core.pipeline.respond import stage
from astrbot.core.utils.metrics import Metric

PL_VERSION = "1.1.5"

SOURCE_DISPLAY = {
    "tencent": "QQ音乐",
    "netease": "网易云音乐",
    "kugou": "酷狗音乐",
    "kuwo": "酷我音乐",
}
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120)
CHUNK_SIZE = 8192
MAX_SESSION_AGE = 3600
AUDIO_CONTENT_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/wav",
    "audio/x-wav",
    "audio/ogg",
    "audio/x-m4a",
    "audio/mp4",
    "audio/x-matroska",
    "application/octet-stream",
}
TEMP_FILE_PREFIX = "astrbot_meting_plugin_"
FFMPEG_INFO_TIMEOUT = 30  # seconds, for -i probe only
FFMPEG_CONVERT_TIMEOUT = 120  # seconds, for format conversion / segmentation
_HTTPS_SCHEME_RE = re.compile(r"^http://", re.IGNORECASE)
PHP_API_SUPPORTED_URLS = {
    "https://metingapi.nanorocky.top/",
    "https://api.injahow.cn/meting/",
    "https://metingapi.mo-app.cn/",
}


def _force_https(url: str) -> str:
    """Replace the URL scheme with https, only at the start of the string."""
    return _HTTPS_SCHEME_RE.sub("https://", url, count=1)


def _generate_guid() -> str:
    """生成基于 machine-id 和 MAC 和 AstrBot 安装 ID 的 GUID"""
    try:
        mid = machineid.id()
    except Exception:
        mid = ""
    return hashlib.md5(
        f"{str(mid)}{str(uuid.getnode())}{Metric.get_installation_id()}".encode()
    ).hexdigest()


class MetingPluginError(Exception):
    """插件基础异常"""

    pass


class DownloadError(MetingPluginError):
    """下载错误"""

    pass


class AudioFormatError(MetingPluginError):
    """音频格式错误"""

    pass


T = TypeVar("T")


@register("astrbot_plugin_meting", "chuyegzs", "基于 MetingAPI 的点歌插件", PL_VERSION)
class MetingPlugin(Star):
    """MetingAPI 点歌插件

    支持多音源搜索和播放，自动分段发送长歌曲
    """

    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config
        self._sessions: dict[str, SessionData] = {}
        self._http_session: aiohttp.ClientSession | None = None
        self._ffmpeg_path = ffmpeg.get_ffmpeg_exe()
        self._download_semaphore: asyncio.Semaphore | None = None
        self._initialized = False
        self._init_lock: asyncio.Lock | None = None

    async def _ensure_initialized(self):
        """确保插件已初始化"""
        if self._initialized:
            return

        if self._init_lock is None:
            self._init_lock = asyncio.Lock()

        async with self._init_lock:
            if self._initialized:
                return

            logger.info("MetingAPI 点歌插件正在初始化...")

            self._download_semaphore = asyncio.Semaphore(3)

            if not self._http_session:
                self._http_session = aiohttp.ClientSession(
                    timeout=REQUEST_TIMEOUT,
                    # 标识请求来源
                    headers={
                        "Referer": "https://astrbot.app/",
                        "User-Agent": f"AstrBot/{VERSION}",
                        "UAK": f"AstrBot/plugin_meting * {PL_VERSION} ",
                        "UUID": _generate_guid(),
                    },
                )

            self._initialized = True
            logger.info("MetingAPI 点歌插件初始化完成")



    async def initialize(self):
        """插件初始化（框架调用）"""
        await self._ensure_initialized()

    def _get_config(
        self, key: str, default: T, validator: Callable[[Any], Any] | None = None
    ) -> T:
        """获取配置值，支持类型和范围校验

        Args:
            key: 配置键
            default: 默认值
            validator: 校验函数，接受配置值，返回校验是否通过

        Returns:
            配置值或默认值
        """
        if not self.config:
            return default

        value = self.config.get(key, default)
        if validator is not None and not validator(value):
            return default

        return value

    def _get_group_config(
        self,
        group: str,
        key: str,
        default: T,
        validator: Callable[[Any], Any] | None = None,
    ) -> T:
        group_conf = self._get_config(group, {}, lambda x: isinstance(x, dict))
        value = group_conf.get(key, default)
        if validator is not None and not validator(value):
            return default
        return value

    def _get_api_config(self) -> dict:
        """获取 API 配置字典"""
        return self._get_config("api_config", {}, lambda x: isinstance(x, dict))

    def get_api_url(self) -> str:
        """获取 API 地址

        Returns:
            str: API 地址，如果未配置则返回空字符串
        """
        api_config = self._get_api_config()
        api_url = api_config.get("api_url", "https://musicapi.chuyel.top/meting/")
        if api_url == "custom":
            # 仅当选择了自定义 API 类型时才使用 custom_api_url 配置项
            url = api_config.get("custom_api_url", "")
            if not url:
                logger.warning(
                    "API 地址设置为 custom 但未填写 custom_api_url，将回退到默认接口"
                )
                url = "https://musicapi.chuyel.top/meting/"
        else:
            url = api_url
        if not url:
            return ""
        url = _force_https(url)
        return url if url.endswith("/") else f"{url}/"

    def get_api_type(self) -> int:
        """获取 API 类型

        Returns:
            int: API 类型，1=Node API, 2=PHP API, 3=自定义参数
        """
        api_config = self._get_api_config()
        api_url = api_config.get("api_url", "https://musicapi.chuyel.top/meting/")
        if api_url == "custom":
            if not api_config.get("custom_api_url", ""):
                return 1

            # 仅当选择了自定义 API 地址时才使用 api_type 配置项
            api_type = api_config.get("api_type", 1)
            api_type = (
                api_type if isinstance(api_type, int) and api_type in (1, 2) else 1
            )
            return api_type

        if api_url in PHP_API_SUPPORTED_URLS:
            return 2
        return 1

    def get_send_music_info(self) -> int:
        """获取音质设置 (br) - 硬编码为999（无损）"""
        return 999

    def get_default_source(self) -> str:
        """获取默认音源

        Returns:
            str: 默认音源，默认为 netease
        """
        return self._get_group_config(
            "search_config", "default_source", "netease", lambda x: x in SOURCE_DISPLAY
        )

    def get_search_result_count(self) -> int:
        """获取搜索结果显示数量

        Returns:
            int: 搜索结果显示数量，范围 5-30，默认 10
        """
        return self._get_group_config(
            "search_config",
            "search_result_count",
            10,
            lambda x: isinstance(x, int) and 5 <= x <= 30,
        )

    def get_max_file_size(self) -> int:
        """获取最大文件大小

        Returns:
            int: 最大文件大小（字节），默认 80MB = 83886080 字节
        """
        try:
            mb = self._get_group_config(
                "download_config",
                "max_file_size",
                80,
                lambda x: isinstance(x, (int, float)) and 10 <= x <= 200,
            )
            if not isinstance(mb, (int, float)):
                logger.warning(f"max_file_size 配置无效: {mb}，使用默认值 80")
                mb = 80
            return int(mb) * 1024 * 1024
        except Exception as e:
            logger.error(f"获取 max_file_size 配置时出错: {e}，使用默认值 80MB")
            return 80 * 1024 * 1024

    async def _perform_search(self, keyword: str, source: str) -> list | None:
        """执行搜索并返回结果列表"""
        api_url = self.get_api_url()
        api_type = self.get_api_type()

        try:
            if not self._http_session:
                return None

            if api_type == 2:
                params = {
                    "server": source,
                    "type": "search",
                    "id": "0",
                    "dwrc": "false",
                    "keyword": keyword,
                }
                api_endpoint = api_url
                logger.info(f"[搜歌] PHP API URL: {api_endpoint}, 参数: {params}")
            else:
                params = {"server": source, "type": "search", "id": keyword}
                api_endpoint = f"{api_url}api"
                logger.info(f"[搜歌] Node API URL: {api_endpoint}, 参数: {params}")

                async with self._http_session.get(api_endpoint, params=params) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()

            if not isinstance(data, list) or not data:
                return []

            result_count = self.get_search_result_count()
            return data[:result_count]

        except Exception as e:
            logger.error(f"搜索歌曲时发生错误: {e}", exc_info=True)
            return None

    async def _download_song(
        self, url: str, sender_id: str, source: str = "", song_id: str = ""
    ) -> tuple[str, float]:
        """下载歌曲文件，验证音频有效性，并按需缓存。

        Args:
            url: 歌曲 URL
            sender_id: 发送者 ID
            source: 歌曲来源
            song_id: 歌曲 ID

        Returns:
            tuple[str, float]: (临时文件路径, 音频时长秒数)

        Raises:
            DownloadError: 下载失败
            AudioFormatError: 下载的文件不是有效音频
        """
        url = _force_https(url)
        http_session = self._http_session
        if not http_session:
            raise DownloadError("HTTP session 未初始化")

        temp_dir = tempfile.gettempdir()
        cache_dir = os.path.join(temp_dir, "astrbot_meting_cache")
        safe_sender_id = "".join(c for c in str(sender_id) if c.isalnum() or c in "._-")

        cache_enabled = self._get_group_config(
            "download_config",
            "enable_cache",
            False,
            lambda x: isinstance(x, bool),
        )

        url_hash = ""
        if cache_enabled:
            cache_key = f"{source}_{song_id}" if source and song_id else url
            url_hash = hashlib.md5(cache_key.encode()).hexdigest()
            os.makedirs(cache_dir, exist_ok=True)
            for filename in os.listdir(cache_dir):
                if filename.startswith(f"{url_hash}_"):
                    try:
                        duration_str = filename.split("_")[1].rsplit(".", 1)[0]
                        duration = float(duration_str)
                        cached_file = os.path.join(cache_dir, filename)
                        logger.info(
                            f"命中缓存，跳过下载和文件检查，直接使用: {cached_file}"
                        )
                        return cached_file, duration
                    except Exception as e:
                        logger.warning(f"读取缓存文件失败: {e}")

        download_success = False
        max_retries = 3
        retry_count = 0
        temp_file = None

        while retry_count < max_retries:
            try:
                if self._download_semaphore is None:
                    raise DownloadError("下载限流器未初始化")
                semaphore = self._download_semaphore
                async with semaphore:
                    logger.debug(
                        f"开始下载歌曲 (尝试 {retry_count + 1}/{max_retries}): {url}"
                    )

                    async with http_session.get(url, allow_redirects=True) as resp:
                        if resp.status != 200:
                            if resp.status >= 500:
                                raise aiohttp.ClientError(
                                    f"上游服务器错误，状态码: {resp.status}"
                                )
                            else:
                                raise DownloadError(f"下载失败，状态码: {resp.status}")

                        content_type = resp.headers.get("Content-Type", "")
                        if not self._is_audio_content(content_type):
                            raise AudioFormatError(
                                f"不支持的 Content-Type: {content_type}"
                            )

                        file_ext = self._guess_file_extension(url, resp.headers)
                        max_file_size_bytes = self.get_max_file_size()
                        max_file_size_mb = max_file_size_bytes // (1024 * 1024)
                        total_size = 0
                        temp_file = os.path.join(
                            temp_dir,
                            f"{TEMP_FILE_PREFIX}{safe_sender_id}_{uuid.uuid4()}{file_ext}",
                        )

                        with open(temp_file, "wb") as f:
                            try:
                                async for chunk in resp.content.iter_chunked(
                                    CHUNK_SIZE
                                ):
                                    f.write(chunk)
                                    total_size += len(chunk)
                                    if total_size > max_file_size_bytes:
                                        raise DownloadError(
                                            f"文件过大，已超过 {max_file_size_mb} MB"
                                        )
                            except aiohttp.ClientPayloadError as e:
                                logger.warning(f"下载时连接断开: {e}")
                                raise e

                        file_size_bytes = os.path.getsize(temp_file)
                        if file_size_bytes == 0:
                            raise DownloadError("下载的文件为空")
                        file_size_mb = file_size_bytes / (1024 * 1024)
                        logger.info(
                            f"歌曲下载成功，临时文件: {temp_file}，文件大小: {file_size_mb:.2f} MB"
                        )

                        # Validate audio and get duration
                        duration = await self._get_audio_info(temp_file)
                        if duration is None or duration <= 0:
                            raise AudioFormatError("下载的文件不是有效音频")

                        logger.debug(f"音频验证通过，时长: {duration:.2f}秒")

                        if cache_enabled:
                            try:
                                cached_filename = f"{url_hash}_{duration:.2f}{file_ext}"
                                cached_file = os.path.join(cache_dir, cached_filename)
                                shutil.move(temp_file, cached_file)
                                logger.debug(f"已缓存音频到 {cached_file}")
                                temp_file = cached_file

                                # 进行缓存大小限制
                                asyncio.create_task(self._enforce_cache_size(cache_dir))
                            except Exception as e:
                                logger.warning(f"缓存音频失败: {e}")

                        download_success = True
                        return temp_file, duration

            except (aiohttp.ClientError, aiohttp.ClientPayloadError) as e:
                retry_count += 1
                logger.error(
                    f"下载歌曲时网络错误 (尝试 {retry_count}/{max_retries}): {e}"
                )
                if retry_count >= max_retries:
                    raise DownloadError(f"网络错误: {e}") from e
                await asyncio.sleep(1)
            except (DownloadError, AudioFormatError):
                raise
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"下载歌曲时发生错误: {e}", exc_info=True)
                raise DownloadError(f"下载失败: {e}") from e
            finally:
                if not download_success and temp_file and os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                        logger.debug("清理临时文件")
                    except Exception:
                        pass

        raise DownloadError("下载失败：已达最大重试次数")

    def _guess_file_extension(self, url: str, headers) -> str:
        """综合嗅探音频文件后缀名

        按照浏览器级别的逻辑降级嗅探文件后缀名：
        1. URL 显式后缀
        2. Content-Disposition 响应头
        3. Content-Type 响应头 (MIME 映射)

        Args:
            url: 下载产生的最终 HTTP URL (或重定向后的 URL)
            headers: aiohttp 响应头

        Returns:
            str: 嗅探得到的文件扩展名，例如 '.mp3', '.flac'，保底返回 '.tmp'
        """
        file_ext = ""
        valid_exts = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aac", ".wma", ".ape"}
        parsed_path = urlparse(url).path
        url_ext = os.path.splitext(parsed_path)[1].lower()
        if url_ext in valid_exts:
            file_ext = url_ext
        if not file_ext:
            cd = headers.get("Content-Disposition", "")
            if "filename=" in cd:
                m = re.search(r'filename=["\']?([^";\']+)', cd)
                if m:
                    cd_ext = os.path.splitext(m.group(1))[1].lower()
                    if cd_ext in valid_exts:
                        file_ext = cd_ext
        if not file_ext:
            content_type = headers.get("Content-Type", "")
            mime_pure = content_type.lower().split(";")[0].strip()
            if mime_pure in ("audio/flac", "audio/x-flac", "application/x-flac"):
                file_ext = ".flac"
            elif mime_pure in ("audio/wav", "audio/x-wav"):
                file_ext = ".wav"
            elif mime_pure in ("audio/mpeg", "audio/mp3"):
                file_ext = ".mp3"
            elif mime_pure in ("audio/mp4", "audio/x-m4a"):
                file_ext = ".m4a"
            elif mime_pure in ("audio/ogg", "application/ogg"):
                file_ext = ".ogg"
            else:
                file_ext = mimetypes.guess_extension(mime_pure) or ".tmp"

        return file_ext

    def _is_audio_content(self, content_type: str) -> bool:
        """判断 Content-Type 是否为音频

        Args:
            content_type: Content-Type 头

        Returns:
            bool: 是否为音频
        """
        if not content_type:
            return False
        content_type_lower = content_type.lower().split(";")[0].strip()
        return content_type_lower in AUDIO_CONTENT_TYPES

    async def _run_ffmpeg(
        self, process: asyncio.subprocess.Process, timeout: int
    ) -> bytes:
        """Wait for an ffmpeg process with a timeout.

        Args:
            process: the running ffmpeg subprocess
            timeout: timeout in seconds

        Returns:
            stderr output (bytes) from ffmpeg.

        Raises:
            asyncio.TimeoutError: if the process did not finish within the timeout.
        """
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            return stderr
        except asyncio.TimeoutError:
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass
            raise

    async def _get_audio_info(self, file_path: str) -> float | None:
        """使用 FFmpeg 获取音频时长。如果不是有效音频，返回 None。"""
        if not self._ffmpeg_path:
            return None
        process = await asyncio.create_subprocess_exec(
            self._ffmpeg_path,
            "-i",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stderr = await self._run_ffmpeg(process, FFMPEG_INFO_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(f"FFmpeg 获取音频信息超时: {file_path}")
            return None
        output = stderr.decode("utf-8", errors="ignore")
        match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", output)
        if match:
            hours, minutes, seconds = map(float, match.groups())
            return hours * 3600 + minutes * 60 + seconds
        return None

    async def _split_and_send_audio(
        self, event: AstrMessageEvent, temp_file: str, session_id: str, duration: float
    ):
        """处理、分割并发送音频

        Args:
            event: 消息事件
            temp_file: 已验证的音频文件路径
            session_id: 会话 ID
            duration: 音频时长（秒），由 _download_song 预先获取
        """
        temp_files_to_cleanup = []
        cache_dir = os.path.join(tempfile.gettempdir(), "astrbot_meting_cache")
        if not os.path.normcase(os.path.abspath(temp_file)).startswith(
            os.path.normcase(os.path.abspath(cache_dir))
        ):
            temp_files_to_cleanup.append(temp_file)

        try:
            if not self._ffmpeg_path:
                logger.error("FFmpeg 调用失败")
                await event.send(event.plain_result("音频处理组件依赖加载失败。"))
                return

            try:
                    logger.debug(
                        f"开始处理音频文件: {temp_file}，时长: {duration:.2f}秒"
                    )

                    # 转换压缩为高压缩率通用格式以减小发送体积
                    base_name = os.path.splitext(os.path.basename(temp_file))[0]
                    # 确保它带有完整前缀并放在临时目录，避免原先可能有后缀名或路径冲突
                    if not base_name.startswith(TEMP_FILE_PREFIX):
                        base_name = f"{TEMP_FILE_PREFIX}{base_name}"

                    processed_file = os.path.join(
                        tempfile.gettempdir(), f"{base_name}_processed.wav"
                    )
                    temp_files_to_cleanup.append(processed_file)

                    process = await asyncio.create_subprocess_exec(
                        self._ffmpeg_path,
                        "-i",
                        temp_file,
                        "-y",
                        "-ar",
                        "24000",
                        "-ac",
                        "1",
                        processed_file,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        await self._run_ffmpeg(process, FFMPEG_CONVERT_TIMEOUT)
                    except asyncio.TimeoutError:
                        logger.error("音频转码超时，尝试清理损坏的文件。")
                        if temp_file and os.path.exists(temp_file):
                            try:
                                os.remove(temp_file)
                            except Exception:
                                pass
                        await event.send(event.plain_result("音频转换超时"))
                        return

                    if process.returncode != 0 or not os.path.exists(processed_file):
                        logger.error("音频转码失败，尝试清理损坏的文件。")
                        if temp_file and os.path.exists(temp_file):
                            try:
                                os.remove(temp_file)
                                logger.debug(f"已清理损坏媒体文件: {temp_file}")
                            except Exception:
                                pass
                        await event.send(event.plain_result("音频转换失败"))
                        return

                    # 直接发送完整音频，不分段
                    logger.debug("直接发送完整音频")
                    await event.send(
                        event.chain_result([Record.fromFileSystem(processed_file)])
                    )

            except asyncio.CancelledError:
                logger.info("音频处理任务被取消")
                await event.send(event.plain_result("音频处理已取消"))
            except Exception as e:
                logger.error(f"分割音频时发生错误: {e}", exc_info=True)
                await event.send(event.plain_result("音频处理失败，请稍后重试"))
        finally:
            for f in temp_files_to_cleanup:
                try:
                    if os.path.exists(f):
                        os.remove(f)
                        logger.debug(f"清理临时文件: {f}")
                except Exception:
                    pass

    async def _play_song_logic(
        self,
        event: AstrMessageEvent,
        song: dict,
        session_id: str,
    ):
        """播放歌曲的通用逻辑 - 简化版：直接语音发送"""
        song_url = song.get("url")

        if not song_url:
            await event.send(event.plain_result("获取歌曲播放地址失败"))
            return

        br = self.get_send_music_info()
        if br > 0 and "type=url" in song_url and "br=" not in song_url:
            connector = "&" if "?" in song_url else "?"
            song_url = f"{song_url}{connector}br={br}"

        source = song.get("source") or await self._get_session_source(session_id)
        song_id = str(song.get("songmid", "")) or str(song.get("id", ""))

        # 直接语音发送模式
        try:
            temp_file, duration = await self._download_song(
                _force_https(song_url), event.get_sender_id(), source, song_id
            )
            await self._split_and_send_audio(event, temp_file, session_id, duration)

        except asyncio.CancelledError:
            logger.info("播放任务被取消")
            await event.send(event.plain_result("播放已取消"))
        except DownloadError as e:
            logger.error(f"下载歌曲失败: {e}")
            await event.send(event.plain_result(f"下载失败: {e}"))
        except AudioFormatError as e:
            logger.error(f"音频格式错误: {e}")
            await event.send(event.plain_result(f"格式不支持: {e}"))
        except Exception as e:
            logger.error(f"播放歌曲时发生错误: {e}", exc_info=True)
            await event.send(event.plain_result("播放失败，请稍后重试"))


    @filter.llm_tool("astr_meting_music")
    async def astr_meting_music(
        self,
        event: AstrMessageEvent,
        keyword: str,
        source: str = "netease",
        index: int = -1,
    ) -> str:
        """这是一个用于搜索和播放音乐的函数。
        搜索音乐：你可以通过提供 keyword (如歌曲名或歌手) 和 source (点歌源： netease, tencent 默认 netease)，不要提供 index (保留为 -1)或将 index 指定为 -1 来进行搜索。函数将返回由搜索到的结果列表（包含序号、歌名、歌手等）的 JSON 数据给你。
        播放音乐：你可以通过提供 keyword, source 以及 index (从 0 开始计数的有效序号)。函数将直接通过插件向用户发送语音。
        至于音乐源和点搜索结果内的哪一首歌，你可以自行判断，也可以询问用户喔。
        注意：点歌成功时函数会返回“点歌任务执行成功！”，此时意味着音乐已发送，这时你无需再进行任何回复。

        Args:
            keyword (string): 搜索关键词（歌手名、歌曲名等）
            source (string): 音乐源，必须是 netease, tencent 之一
            index (number): 歌曲序号。-1 表示仅搜索，0 表示第一首，依次类推
        """
        try:
            # LLM 工具调用常把整数序号传成 0.0 / "0" 等，list 下标需要真正的 int
            try:
                index = int(index)
            except (TypeError, ValueError, OverflowError):
                return f"无效的序号：{index}，请传入整数序号（-1 表示仅搜索）。"

            if source not in SOURCE_DISPLAY:
                return f"不支持的点歌源：{source}，请从 netease, tencent 中选择。"

            results = await self._perform_search(keyword, source)
            if not results:
                return "未搜索到任何相关歌曲。"

            if index < 0:
                summary_results = []
                for i, r in enumerate(results[: self.get_search_result_count()]):
                    title = r.get("name") or r.get("title") or "未知歌名"
                    raw_artist = r.get("artist") or r.get("author") or "未知歌手"
                    raw_album = r.get("album") or "未知专辑"
                    artist_str = (
                        ", ".join(raw_artist)
                        if isinstance(raw_artist, list)
                        else str(raw_artist)
                    )
                    album_str = (
                        ", ".join(raw_album)
                        if isinstance(raw_album, list)
                        else str(raw_album)
                    )
                    item = {
                        "index": i,
                        "name": title,
                        "artist": artist_str,
                        "album": album_str,
                    }
                    if r.get("source"):
                        item["source"] = r.get("source")
                    if r.get("duration"):
                        item["duration"] = r.get("duration")
                    summary_results.append(item)
                return json.dumps(summary_results, ensure_ascii=False)

            if index >= len(results):
                return f"指定的序号 {index} 超出搜索结果范围，最大可选序号为 {len(results) - 1}。"

            target_song = results[index]
            target_song["source"] = source
            session_id = event.unified_msg_origin

            await self._play_song_logic(event, target_song, session_id)

            return "点歌任务执行成功！"

        except Exception as e:
            logger.error(f"音乐搜索/播放失败：{e}", exc_info=True)
            return f"发生了错误：{e}"

    async def terminate(self):
        """插件终止时清理资源"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        self._sessions.clear()
        self._session_audio_locks.clear()
        self._initialized = False
