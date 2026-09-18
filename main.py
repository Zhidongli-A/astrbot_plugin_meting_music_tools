 import asyncio
import json
import os
import re
import tempfile
import uuid

import imageio_ffmpeg as ffmpeg
import requests

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Record
from astrbot.api.star import Context, Star, register

PL_VERSION = "1.1.2"

DEFAULT_API_URL = "https://api.qijieya.cn/meting/"

CHUNK_SIZE = 8192
TEMP_FILE_PREFIX = "astrbot_meting_plugin_"
FFMPEG_INFO_TIMEOUT = 30
FFMPEG_CONVERT_TIMEOUT = 120


class MetingPluginError(Exception):
    pass


class DownloadError(MetingPluginError):
    pass


class AudioFormatError(MetingPluginError):
    pass


@register("astrbot_plugin_meting", "chuyegzs", "基于 MetingAPI 的点歌插件", PL_VERSION)
class MetingPlugin(Star):
    """MetingAPI 点歌插件 - 仅支持网易云"""

    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config
        self._ffmpeg_path = ffmpeg.get_ffmpeg_exe()
        self._http_session: requests.Session | None = None

    def _ensure_session(self) -> requests.Session:
        if self._http_session is None:
            self._http_session = requests.Session()
        return self._http_session

    async def initialize(self):
        logger.info("MetingAPI 点歌插件初始化完成")

    def _get_config(self, key: str, default):
        if not self.config:
            return default
        return self.config.get(key, default)

    def get_api_url(self) -> str:
        url = self._get_config("api_url", DEFAULT_API_URL)
        if not url:
            return DEFAULT_API_URL
        return url if url.endswith("/") else f"{url}/"

    def get_search_result_count(self) -> int:
        count = self._get_config("search_result_count", 10)
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 10
        return count

    def get_max_file_size(self) -> int:
        mb = self._get_config("max_file_size", 80)
        if not isinstance(mb, (int, float)):
            mb = 80
        return int(mb) * 1024 * 1024

    async def _perform_search(self, keyword: str) -> list | None:
        """执行搜索: GET {api_url}?server=netease&type=search&id={keyword}"""
        api_url = self.get_api_url()
        endpoint = api_url
        params = {"server": "netease", "type": "search", "id": keyword}

        try:
            logger.info(f"[搜歌] URL: {endpoint}, 参数: {params}")
            session = self._ensure_session()
            resp = await asyncio.to_thread(
                session.get, endpoint, params=params, timeout=30
            )
            if resp.status_code != 200:
                logger.error(f"搜索失败，状态码: {resp.status_code}")
                return None

            data = resp.json()
            if not isinstance(data, list) or not data:
                return []

            count = self.get_search_result_count()
            return data[:count]
        except Exception as e:
            logger.error(f"搜索歌曲时发生错误: {e}", exc_info=True)
            return None

    async def _download_song(self, url: str, sender_id: str) -> tuple[str, float]:
        """下载歌曲 - 格式固定为 MP3，不进行转码"""
        session = self._ensure_session()
        safe_sender_id = "".join(c for c in str(sender_id) if c.isalnum() or c in "._-")
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(
            temp_dir,
            f"{TEMP_FILE_PREFIX}{safe_sender_id}_{uuid.uuid4()}.mp3",
        )

        try:
            logger.debug(f"开始下载歌曲: {url}")

            def _do_download():
                r = session.get(url, timeout=120, stream=True, allow_redirects=True)
                if r.status_code != 200:
                    r.close()
                    raise DownloadError(f"下载失败，状态码: {r.status_code}")

                max_size = self.get_max_file_size()
                size = 0
                with open(temp_file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
                        size += len(chunk)
                        if size > max_size:
                            r.close()
                            raise DownloadError(
                                f"文件过大，已超过 {max_size // (1024 * 1024)} MB"
                            )
                r.close()

            await asyncio.to_thread(_do_download)

            if os.path.getsize(temp_file) == 0:
                raise DownloadError("下载的文件为空")

            logger.info(
                f"歌曲下载成功: {temp_file}，大小: {os.path.getsize(temp_file) / (1024 * 1024):.2f} MB"
            )

            duration = await self._get_audio_info(temp_file)
            if duration is None or duration <= 0:
                raise AudioFormatError("下载的文件不是有效音频")

            logger.debug(f"音频验证通过，时长: {duration:.2f}秒")
            return temp_file, duration

        except (DownloadError, AudioFormatError):
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
            raise
        except Exception as e:
            logger.error(f"下载歌曲时发生错误: {e}", exc_info=True)
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
            raise DownloadError(f"下载失败: {e}") from e

    async def _get_audio_info(self, file_path: str) -> float | None:
        """使用 FFmpeg 获取音频时长"""
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
            _, stderr = await asyncio.wait_for(
                process.communicate(), timeout=FFMPEG_INFO_TIMEOUT
            )
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
        """处理并发送音频 - 转换为 WAV(24kHz 单声道) 后发送语音"""
        temp_files_to_cleanup = [temp_file]

        try:
            if not self._ffmpeg_path:
                logger.error("FFmpeg 调用失败")
                await event.send(event.plain_result("音频处理组件依赖加载失败。"))
                return

            base_name = os.path.splitext(os.path.basename(temp_file))[0]
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
                await asyncio.wait_for(
                    process.communicate(), timeout=FFMPEG_CONVERT_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.error("音频转码超时，尝试清理损坏的文件。")
                await event.send(event.plain_result("音频转换超时"))
                return

            if process.returncode != 0 or not os.path.exists(processed_file):
                logger.error("音频转码失败，尝试清理损坏的文件。")
                await event.send(event.plain_result("音频转换失败"))
                return

            logger.debug("直接发送完整音频")
            await event.send(
                event.chain_result([Record.fromFileSystem(processed_file)])
            )

        except asyncio.CancelledError:
            logger.info("音频处理任务被取消")
            await event.send(event.plain_result("音频处理已取消"))
        except Exception as e:
            logger.error(f"处理音频时发生错误: {e}", exc_info=True)
            await event.send(event.plain_result("音频处理失败，请稍后重试"))
        finally:
            for f in temp_files_to_cleanup:
                try:
                    if os.path.exists(f):
                        os.remove(f)
                except Exception:
                    pass

    async def _play_song_logic(
        self, event: AstrMessageEvent, song: dict, session_id: str
    ):
        """播放歌曲 - 下载并发送语音"""
        song_url = song.get("url")
        if not song_url:
            await event.send(event.plain_result("获取歌曲播放地址失败"))
            return

        try:
            temp_file, duration = await self._download_song(
                song_url, event.get_sender_id()
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
        index: int = -1,
    ) -> str:
        """这是一个用于搜索和播放网易云音乐的函数。
        搜索音乐：提供 keyword (如歌曲名或歌手)，index 保留为 -1 或指定为 -1。返回搜索结果列表（包含序号、歌名、歌手等）的 JSON。
        播放音乐：提供 keyword 和 index (从 0 开始的序号)。直接发送语音。
        注意：点歌成功时返回"点歌任务执行成功！"，此时音乐已发送，无需再回复。

        Args:
            keyword (string): 搜索关键词（歌手名、歌曲名等）
            index (number): 歌曲序号。-1 表示仅搜索，0 表示第一首，依次类推
        """
        try:
            try:
                index = int(index)
            except (TypeError, ValueError, OverflowError):
                return f"无效的序号：{index}，请传入整数序号（-1 表示仅搜索）。"

            results = await self._perform_search(keyword)
            if not results:
                return "未搜索到任何相关歌曲。"

            if index < 0:
                summary_results = []
                for i, r in enumerate(results[: self.get_search_result_count()]):
                    title = r.get("name") or r.get("title") or "未知歌名"
                    raw_artist = r.get("artist") or r.get("author") or "未知歌手"
                    artist_str = (
                        ", ".join(raw_artist)
                        if isinstance(raw_artist, list)
                        else str(raw_artist)
                    )
                    item = {"index": i, "name": title, "artist": artist_str}
                    summary_results.append(item)
                return json.dumps(summary_results, ensure_ascii=False)

            if index >= len(results):
                return f"指定的序号 {index} 超出搜索结果范围，最大可选序号为 {len(results) - 1}。"

            target_song = results[index]
            session_id = event.unified_msg_origin

            await self._play_song_logic(event, target_song, session_id)
            return "点歌任务执行成功！"

        except Exception as e:
            logger.error(f"音乐搜索/播放失败：{e}", exc_info=True)
            return f"发生了错误：{e}"

    async def terminate(self):
        """插件终止时清理资源"""
        if self._http_session:
            try:
                self._http_session.close()
            except Exception:
                pass
            self._http_session = None
