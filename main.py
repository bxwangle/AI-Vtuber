import os
import threading
import schedule
import random
import asyncio, aiohttp
import traceback
import copy
import json, re

from functools import partial

import http.cookies
from typing import *

# 按键监听语音聊天板块
import keyboard
import pyaudio
import wave
import numpy as np
import speech_recognition as sr
from aip import AipSpeech
import signal
import time

import http.server
import socketserver

from utils.my_log import logger
from utils.common import Common
from utils.config import Config
from utils.my_handle import My_handle

"""
	___ _                       
	|_ _| | ____ _ _ __ ___  ___ 
	 | || |/ / _` | '__/ _ \/ __|
	 | ||   < (_| | | | (_) \__ \
	|___|_|\_\__,_|_|  \___/|___/

"""

config = None
common = None
my_handle = None
last_liveroom_data = None
last_username_list = None
# 空闲时间计数器
global_idle_time = 0

# 配置文件路径
config_path = "config.json"


# web服务线程
async def web_server_thread(web_server_port):
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", web_server_port), Handler) as httpd:
        logger.info(f"Web运行在端口：{web_server_port}")
        logger.info(
            f"可以直接访问Live2D页， http://127.0.0.1:{web_server_port}/Live2D/"
        )
        httpd.serve_forever()


"""
                       _oo0oo_
                      o8888888o
                      88" . "88
                      (| -_- |)
                      0\  =  /0
                    ___/`---'\___
                  .' \\|     |// '.
                 / \\|||  :  |||// \
                / _||||| -:- |||||- \
               |   | \\\  - /// |   |
               | \_|  ''\---/''  |_/ |
               \  .-\__  '-'  ___/-. /
             ___'. .'  /--.--\  `. .'___
          ."" '<  `.___\_<|>_/___.' >' "".
         | | :  `- \`.;`\ _ /`;.`/ - ` : | |
         \  \ `_.   \_ __\ /__ _/   .-` /  /
     =====`-.____`.___ \_____/___.-`___.-'=====
                       `=---='


     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

         佛祖保佑       永不宕机     永无BUG
"""


# 点火起飞
def start_server():
    global \
        config, \
        common, \
        my_handle, \
        last_username_list, \
        config_path, \
        last_liveroom_data
    global do_listen_and_comment_thread, stop_do_listen_and_comment_thread_event
    global faster_whisper_model, sense_voice_model, is_recording, is_talk_awake, wait_play_audio_num

    # 按键监听相关
    do_listen_and_comment_thread = None
    stop_do_listen_and_comment_thread_event = threading.Event()
    # 冷却时间 0.5 秒
    cooldown = 0.5
    last_pressed = 0
    # 正在录音中 标志位
    is_recording = False
    # 聊天是否唤醒
    is_talk_awake = False

    # 待播放音频数量（在使用 音频播放器 或者 metahuman-stream等不通过AI Vtuber播放音频的对接项目时，使用此变量记录是是否还有音频没有播放完）
    wait_play_audio_num = 0

    # 获取 httpx 库的日志记录器
    # httpx_logger = logging.getLogger("httpx")
    # 设置 httpx 日志记录器的级别为 WARNING
    # httpx_logger.setLevel(logging.WARNING)

    # 最新的直播间数据
    last_liveroom_data = {
        "OnlineUserCount": 0,
        "TotalUserCount": 0,
        "TotalUserCountStr": "0",
        "OnlineUserCountStr": "0",
        "MsgId": 0,
        "User": None,
        "Content": "当前直播间人数 0，累计直播间人数 0",
        "RoomId": 0,
    }
    # 最新入场的用户名列表
    last_username_list = [""]

    my_handle = My_handle(config_path)
    if my_handle is None:
        logger.error("程序初始化失败！")
        os._exit(0)

    # Live2D线程
    try:
        if config.get("live2d", "enable"):
            web_server_port = int(config.get("live2d", "port"))
            threading.Thread(
                target=lambda: asyncio.run(web_server_thread(web_server_port))
            ).start()
    except Exception as e:
        logger.error(traceback.format_exc())
        os._exit(0)

    if platform != "wxlive":
        """

                  /@@@@@@@@          @@@@@@@@@@@@@@@].      =@@@@@@@       
                 =@@@@@@@@@^         @@@@@@@@@@@@@@@@@@`    =@@@@@@@       
                ,@@@@@@@@@@@`        @@@@@@@@@@@@@@@@@@@^   =@@@@@@@       
               .@@@@@@\@@@@@@.       @@@@@@@^   .\@@@@@@\   =@@@@@@@       
               /@@@@@/ \@@@@@\       @@@@@@@^    =@@@@@@@   =@@@@@@@       
              =@@@@@@. .@@@@@@^      @@@@@@@\]]]@@@@@@@@^   =@@@@@@@       
             ,@@@@@@^   =@@@@@@`     @@@@@@@@@@@@@@@@@@/    =@@@@@@@       
            .@@@@@@@@@@@@@@@@@@@.    @@@@@@@@@@@@@@@@/`     =@@@@@@@       
            /@@@@@@@@@@@@@@@@@@@\    @@@@@@@^               =@@@@@@@       
           =@@@@@@@@@@@@@@@@@@@@@^   @@@@@@@^               =@@@@@@@       
          ,@@@@@@@.       ,@@@@@@@`  @@@@@@@^               =@@@@@@@       
          @@@@@@@^         =@@@@@@@. @@@@@@@^               =@@@@@@@   

        """
        
        # HTTP API线程
        def http_api_thread():
            import uvicorn
            from fastapi import FastAPI
            from fastapi.middleware.cors import CORSMiddleware
            from utils.models import (
                SendMessage,
                LLMMessage,
                CallbackMessage,
                CommonResult,
            )

            # 定义FastAPI应用
            app = FastAPI()

            # 允许跨域
            app.add_middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )

            # 定义POST请求路径和处理函数
            @app.post("/send")
            async def send(msg: SendMessage):
                global my_handle, config

                try:
                    tmp_json = msg.dict()
                    logger.info(f"内部HTTP API send接口收到数据：{tmp_json}")
                    data_json = tmp_json["data"]
                    if "type" not in data_json:
                        data_json["type"] = tmp_json["type"]

                    if data_json["type"] in ["reread", "reread_top_priority"]:
                        my_handle.reread_handle(data_json, type=data_json["type"])
                    elif data_json["type"] == "comment":
                        my_handle.process_data(data_json, "comment")
                    elif data_json["type"] == "tuning":
                        my_handle.tuning_handle(data_json)
                    elif data_json["type"] == "gift":
                        my_handle.gift_handle(data_json)
                    elif data_json["type"] == "entrance":
                        my_handle.entrance_handle(data_json)

                    return CommonResult(code=200, message="成功")
                except Exception as e:
                    logger.error(f"发送数据失败！{e}")
                    return CommonResult(code=-1, message=f"发送数据失败！{e}")

            @app.post("/llm")
            async def llm(msg: LLMMessage):
                global my_handle, config

                try:
                    data_json = msg.dict()
                    logger.info(f"API收到数据：{data_json}")

                    resp_content = my_handle.llm_handle(
                        data_json["type"], data_json, webui_show=False
                    )

                    return CommonResult(
                        code=200, message="成功", data={"content": resp_content}
                    )
                except Exception as e:
                    logger.error(f"调用LLM失败！{e}")
                    return CommonResult(code=-1, message=f"调用LLM失败！{e}")

            @app.post("/callback")
            async def callback(msg: CallbackMessage):
                global my_handle, config, global_idle_time, wait_play_audio_num

                try:
                    data_json = msg.dict()

                    # 特殊回调特殊处理
                    if data_json["type"] == "audio_playback_completed":
                        wait_play_audio_num = int(data_json["data"]["wait_play_audio_num"])
                        wait_synthesis_msg_num = int(data_json["data"]["wait_synthesis_msg_num"])
                        logger.info(f"内部HTTP API callback接口 音频播放完成回调，待播放音频数量：{wait_play_audio_num}，待合成消息数量：{wait_synthesis_msg_num}")
                    else:
                        logger.info(f"内部HTTP API callback接口收到数据：{data_json}")

                    # 音频播放完成
                    if data_json["type"] in ["audio_playback_completed"]:
                        wait_play_audio_num = int(data_json["data"]["wait_play_audio_num"])

                        # 如果等待播放的音频数量大于10
                        if data_json["data"]["wait_play_audio_num"] > int(
                            config.get(
                                "idle_time_task", "wait_play_audio_num_threshold"
                            )
                        ):
                            logger.info(
                                f'等待播放的音频数量大于限定值，闲时任务的闲时计时由 {global_idle_time} -> {int(config.get("idle_time_task", "idle_time_reduce_to"))}秒'
                            )
                            # 闲时任务的闲时计时 清零
                            global_idle_time = int(
                                config.get("idle_time_task", "idle_time_reduce_to")
                            )

                    return CommonResult(code=200, message="callback处理成功！")
                except Exception as e:
                    logger.error(f"callback处理失败！{e}")
                    return CommonResult(code=-1, message=f"callback处理失败！{e}")

            logger.info("HTTP API线程已启动！")
            uvicorn.run(app, host="0.0.0.0", port=config.get("api_port"))

        # HTTP API线程并启动
        inside_http_api_thread = threading.Thread(target=http_api_thread)
        inside_http_api_thread.start()

    # 添加用户名到最新的用户名列表
    def add_username_to_last_username_list(data):
        """
        data(str): 用户名
        """
        global last_username_list

        # 添加数据到 最新入场的用户名列表
        last_username_list.append(data)

        # 保留最新的3个数据
        last_username_list = last_username_list[-3:]

    """
    按键监听板块
    """

    # 录音功能(录音时间过短进入openai的语音转文字会报错，请一定注意)
    def record_audio():
        pressdown_num = 0
        CHUNK = 1024
        FORMAT = pyaudio.paInt16
        CHANNELS = 1
        RATE = 44100
        WAVE_OUTPUT_FILENAME = "out/record.wav"
        p = pyaudio.PyAudio()
        stream = p.open(
            format=FORMAT,
            channels=CHANNELS,
            rate=RATE,
            input=True,
            frames_per_buffer=CHUNK,
        )
        frames = []
        logger.info("Recording...")
        flag = 0
        while 1:
            while keyboard.is_pressed("RIGHT_SHIFT"):
                flag = 1
                data = stream.read(CHUNK)
                frames.append(data)
                pressdown_num = pressdown_num + 1
            if flag:
                break
        logger.info("Stopped recording.")
        stream.stop_stream()
        stream.close()
        p.terminate()
        wf = wave.open(WAVE_OUTPUT_FILENAME, "wb")
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(p.get_sample_size(FORMAT))
        wf.setframerate(RATE)
        wf.writeframes(b"".join(frames))
        wf.close()
        if pressdown_num >= 5:  # 粗糙的处理手段
            return 1
        else:
            logger.info("杂鱼杂鱼，好短好短(录音时间过短,按右shift重新录制)")
            return 0

    # THRESHOLD 设置音量阈值,默认值800.0,根据实际情况调整  silence_threshold 设置沉默阈值，根据实际情况调整
    def audio_listen(volume_threshold=800.0, silence_threshold=15):
        audio = pyaudio.PyAudio()

        # 设置音频参数
        FORMAT = pyaudio.paInt16
        CHANNELS = config.get("talk", "CHANNELS")
        RATE = config.get("talk", "RATE")
        CHUNK = 1024

        stream = audio.open(
            format=FORMAT,
            channels=CHANNELS,
            rate=RATE,
            input=True,
            frames_per_buffer=CHUNK,
            input_device_index=int(config.get("talk", "device_index")),
        )

        frames = []  # 存储录制的音频帧

        is_speaking = False  # 是否在说话
        silent_count = 0  # 沉默计数
        speaking_flag = False  # 录入标志位 不重要

        logger.info("[即将开始录音……]")

        while True:
            # 播放中不录音
            if config.get("talk", "no_recording_during_playback"):
                # 存在待合成音频 或 已合成音频还未播放 或 播放中 或 在数据处理中
                if (
                    my_handle.is_audio_queue_empty() != 15
                    or my_handle.is_handle_empty() == 1
                    or wait_play_audio_num > 0
                ):
                    time.sleep(
                        float(
                            config.get(
                                "talk", "no_recording_during_playback_sleep_interval"
                            )
                        )
                    )
                    continue

            # 读取音频数据
            data = stream.read(CHUNK)
            audio_data = np.frombuffer(data, dtype=np.short)
            max_dB = np.max(audio_data)
            # logger.info(max_dB)
            if max_dB > volume_threshold:
                is_speaking = True
                silent_count = 0
            elif is_speaking is True:
                silent_count += 1

            if is_speaking is True:
                frames.append(data)
                if speaking_flag is False:
                    logger.info("[录入中……]")
                    speaking_flag = True

            if silent_count >= silence_threshold:
                break

        logger.info("[语音录入完成]")

        # 将音频保存为WAV文件
        """with wave.open(WAVE_OUTPUT_FILENAME, 'wb') as wf:
            wf.setnchannels(CHANNELS)
            wf.setsampwidth(pyaudio.get_sample_size(FORMAT))
            wf.setframerate(RATE)
            wf.writeframes(b''.join(frames))"""
        return frames

    # 处理聊天逻辑 传入ASR后的文本内容
    def talk_handle(content: str):
        global is_talk_awake

        def clear_queue_and_stop_audio_play(message_queue: bool=True, voice_tmp_path_queue: bool=True, stop_audio_play: bool=True):
            """
            清空队列 或 停止播放音频
            """
            if message_queue:
                ret = my_handle.clear_queue("message_queue")
                if ret:
                    logger.info("清空待合成消息队列成功！")
                else:
                    logger.error("清空待合成消息队列失败！")
            if voice_tmp_path_queue:
                ret = my_handle.clear_queue("voice_tmp_path_queue")
                if ret:
                    logger.info("清空待播放音频队列成功！")
                else:
                    logger.error("清空待播放音频队列失败！")
            if stop_audio_play:
                ret = my_handle.stop_audio("pygame", True, True)

        try:
            # 检查并切换聊天唤醒状态
            def check_talk_awake(content: str):
                """检查并切换聊天唤醒状态

                Args:
                    content (str): 聊天内容

                Returns:
                    dict:
                        ret 是否需要触发
                        is_talk_awake 当前唤醒状态
                        first 是否是第一次触发 唤醒or睡眠，用于触发首次切换时的特殊提示语
                """
                global is_talk_awake

                # 判断是否启动了 唤醒词功能
                if config.get("talk", "wakeup_sleep", "enable"):
                    if config.get("talk", "wakeup_sleep", "mode") == "长期唤醒":
                        # 判断现在是否是唤醒状态
                        if is_talk_awake is False:
                            # 判断文本内容是否包含唤醒词
                            trigger_word = common.find_substring_in_list(
                                content, config.get("talk", "wakeup_sleep", "wakeup_word")
                            )
                            if trigger_word:
                                is_talk_awake = True
                                logger.info("[聊天唤醒成功]")
                                return {
                                    "ret": 0,
                                    "is_talk_awake": is_talk_awake,
                                    "first": True,
                                    "trigger_word": trigger_word,
                                }
                            return {
                                "ret": -1,
                                "is_talk_awake": is_talk_awake,
                                "first": False,
                            }
                        else:
                            # 判断文本内容是否包含睡眠词
                            trigger_word = common.find_substring_in_list(
                                content, config.get("talk", "wakeup_sleep", "sleep_word")
                            )
                            if trigger_word:
                                is_talk_awake = False
                                logger.info("[聊天睡眠成功]")
                                return {
                                    "ret": 0,
                                    "is_talk_awake": is_talk_awake,
                                    "first": True,
                                    "trigger_word": trigger_word,
                                }
                            return {
                                "ret": 0,
                                "is_talk_awake": is_talk_awake,
                                "first": False,
                            }
                    elif config.get("talk", "wakeup_sleep", "mode") == "单次唤醒":
                        # 无需判断当前是否是唤醒状态，因为默认都是状态清除
                        # 判断文本内容是否包含唤醒词
                        trigger_word = common.find_substring_in_list(
                            content, config.get("talk", "wakeup_sleep", "wakeup_word")
                        )
                        if trigger_word:
                            is_talk_awake = True
                            logger.info("[聊天唤醒成功]")
                            return {
                                "ret": 0,
                                "is_talk_awake": is_talk_awake,
                                # 单次唤醒下 没有首次唤醒提示
                                "first": False,
                                "trigger_word": trigger_word,
                            }
                        return {
                            "ret": -1,
                            "is_talk_awake": is_talk_awake,
                            "first": False,
                        }


                return {"ret": 0, "is_talk_awake": True, "trigger_word": "", "first": False}

            # 输出识别结果
            logger.info("识别结果：" + content)

            # 空内容过滤
            if content == "":
                return

            username = config.get("talk", "username")

            data = {"platform": "本地聊天", "username": username, "content": content}
            
            # 检查并切换聊天唤醒状态
            check_resp = check_talk_awake(content)
            if check_resp["ret"] == 0:
                # 唤醒情况下
                if check_resp["is_talk_awake"]:
                    # 长期唤醒、且不是首次触发的情况下，后面的内容不会携带触发词，即使携带了也不应该进行替换操作
                    if config.get("talk", "wakeup_sleep", "mode") == "长期唤醒" and not check_resp["first"]:
                        pass
                    else:
                        # 替换触发词为空
                        content = content.replace(check_resp["trigger_word"], "").strip()

                    # 因为唤醒可能会有仅唤醒词的情况，所以可能出现首次唤醒，唤醒词被过滤，content为空清空，导致不播放唤醒提示语，需要处理
                    if content == "" and not check_resp["first"]:
                        return
                    
                    # 赋值给data
                    data["content"] = content
                    
                    # 首次触发切换模式 播放唤醒文案
                    if check_resp["first"]:
                        # 随机获取文案 TODO: 如果此功能测试成功，所有的类似功能都将使用此函数简化代码
                        resp_json = common.get_random_str_in_list_and_format(
                            ori_list=config.get(
                                "talk", "wakeup_sleep", "wakeup_copywriting"
                            )
                        )
                        if resp_json["ret"] == 0:
                            data["content"] = resp_json["content"]
                            data["insert_index"] = -1
                            my_handle.reread_handle(data)
                    else:
                        # 如果启用了“打断对话”功能
                        if config.get("talk", "interrupt_talk", "enable"):
                            # 判断文本内容是否包含中断词
                            interrupt_word = common.find_substring_in_list(
                                data["content"], config.get("talk", "interrupt_talk", "keywords")
                            )
                            if interrupt_word:
                                logger.info(f"[聊天中断] 命中中断词：{interrupt_word}")
                                # 从配置中获取需要清除的数据类型
                                clean_type = config.get("talk", "interrupt_talk", "clean_type")
                                # 各类型数据是否清除
                                message_queue = "message_queue" in clean_type
                                voice_tmp_path_queue = "voice_tmp_path_queue" in clean_type
                                stop_audio_play = "stop_audio_play" in clean_type
                                
                                clear_queue_and_stop_audio_play(message_queue, voice_tmp_path_queue, stop_audio_play)
                                return False

                        # 传递给my_handle进行进行后续一系列的处理
                        my_handle.process_data(data, "talk")

                        # 单次唤醒情况下，唤醒后关闭
                        if config.get("talk", "wakeup_sleep", "mode") == "单次唤醒":
                            is_talk_awake = False
                # 睡眠情况下
                else:
                    # 首次进入睡眠 播放睡眠文案
                    if check_resp["first"]:
                        resp_json = common.get_random_str_in_list_and_format(
                            ori_list=config.get(
                                "talk", "wakeup_sleep", "sleep_copywriting"
                            )
                        )
                        if resp_json["ret"] == 0:
                            data["content"] = resp_json["content"]
                            data["insert_index"] = -1
                            my_handle.reread_handle(data)
        except Exception as e:
            logger.error(traceback.format_exc())

    # 执行录音、识别&提交
    def do_listen_and_comment(status=True):
        global \
            stop_do_listen_and_comment_thread_event, \
            faster_whisper_model, \
            sense_voice_model, \
            is_recording, \
            is_talk_awake

        try:
            is_recording = True

            config = Config(config_path)
            # 是否启用按键监听和直接对话，没启用的话就不用执行了
            if not config.get("talk", "key_listener_enable") and not config.get("talk", "direct_run_talk"):
                is_recording = False
                return

            # 针对faster_whisper情况，模型加载一次共用，减少开销
            if "faster_whisper" == config.get("talk", "type"):
                from faster_whisper import WhisperModel

                if faster_whisper_model is None:
                    logger.info("faster_whisper 模型加载中，请稍后...")
                    # Run on GPU with FP16
                    faster_whisper_model = WhisperModel(
                        model_size_or_path=config.get(
                            "talk", "faster_whisper", "model_size"
                        ),
                        device=config.get("talk", "faster_whisper", "device"),
                        compute_type=config.get(
                            "talk", "faster_whisper", "compute_type"
                        ),
                        download_root=config.get(
                            "talk", "faster_whisper", "download_root"
                        ),
                    )
                    logger.info("faster_whisper 模型加载完毕，可以开始说话了喵~")
            elif "sensevoice" == config.get("talk", "type"):
                from funasr import AutoModel

                logger.info("sensevoice 模型加载中，请稍后...")
                asr_model_path = config.get("talk", "sensevoice", "asr_model_path")
                vad_model_path = config.get("talk", "sensevoice", "vad_model_path")
                if sense_voice_model is None:
                    sense_voice_model = AutoModel(
                        model=asr_model_path,
                        vad_model=vad_model_path,
                        vad_kwargs={
                            "max_single_segment_time": int(
                                config.get(
                                    "talk", "sensevoice", "vad_max_single_segment_time"
                                )
                            )
                        },
                        trust_remote_code=True,
                        device=config.get("talk", "sensevoice", "device"),
                        remote_code="./sensevoice/model.py",
                    )

                    logger.info("sensevoice 模型加载完毕，可以开始说话了喵~")

            while True:
                try:
                    # 检查是否收到停止事件
                    if stop_do_listen_and_comment_thread_event.is_set():
                        logger.info("停止录音~")
                        is_recording = False
                        break

                    config = Config(config_path)

                    # 根据接入的语音识别类型执行
                    if config.get("talk", "type") in [
                        "baidu",
                        "faster_whisper",
                        "sensevoice",
                    ]:
                        # 设置音频参数
                        FORMAT = pyaudio.paInt16
                        CHANNELS = config.get("talk", "CHANNELS")
                        RATE = config.get("talk", "RATE")

                        audio_out_path = config.get("play_audio", "out_path")

                        if not os.path.isabs(audio_out_path):
                            if not audio_out_path.startswith("./"):
                                audio_out_path = "./" + audio_out_path
                        file_name = "asr_" + common.get_bj_time(4) + ".wav"
                        WAVE_OUTPUT_FILENAME = common.get_new_audio_path(
                            audio_out_path, file_name
                        )
                        # WAVE_OUTPUT_FILENAME = './out/asr_' + common.get_bj_time(4) + '.wav'

                        frames = audio_listen(
                            config.get("talk", "volume_threshold"),
                            config.get("talk", "silence_threshold"),
                        )

                        # 将音频保存为WAV文件
                        with wave.open(WAVE_OUTPUT_FILENAME, "wb") as wf:
                            wf.setnchannels(CHANNELS)
                            wf.setsampwidth(pyaudio.get_sample_size(FORMAT))
                            wf.setframerate(RATE)
                            wf.writeframes(b"".join(frames))

                        if config.get("talk", "type") == "baidu":
                            # 读取音频文件
                            with open(WAVE_OUTPUT_FILENAME, "rb") as fp:
                                audio = fp.read()

                            # 初始化 AipSpeech 对象
                            baidu_client = AipSpeech(
                                config.get("talk", "baidu", "app_id"),
                                config.get("talk", "baidu", "api_key"),
                                config.get("talk", "baidu", "secret_key"),
                            )

                            # 识别音频文件
                            res = baidu_client.asr(
                                audio,
                                "wav",
                                16000,
                                {
                                    "dev_pid": 1536,
                                },
                            )
                            if res["err_no"] == 0:
                                content = res["result"][0]

                                talk_handle(content)
                            else:
                                logger.error(f"百度接口报错：{res}")
                        elif config.get("talk", "type") == "faster_whisper":
                            logger.debug("faster_whisper模型加载中...")

                            language = config.get("talk", "faster_whisper", "language")
                            if language == "自动识别":
                                language = None

                            segments, info = faster_whisper_model.transcribe(
                                WAVE_OUTPUT_FILENAME,
                                language=language,
                                beam_size=config.get(
                                    "talk", "faster_whisper", "beam_size"
                                ),
                            )

                            logger.debug(
                                "识别语言为：'%s'，概率：%f"
                                % (info.language, info.language_probability)
                            )

                            content = ""
                            for segment in segments:
                                logger.info(
                                    "[%.2fs -> %.2fs] %s"
                                    % (segment.start, segment.end, segment.text)
                                )
                                content += segment.text + "。"

                            if content == "":
                                # 恢复录音标志位
                                is_recording = False
                                return

                            talk_handle(content)
                        elif config.get("talk", "type") == "sensevoice":
                            res = sense_voice_model.generate(
                                input=WAVE_OUTPUT_FILENAME,
                                cache={},
                                language=config.get("talk", "sensevoice", "language"),
                                text_norm=config.get("talk", "sensevoice", "text_norm"),
                                batch_size_s=int(
                                    config.get("talk", "sensevoice", "batch_size_s")
                                ),
                                batch_size=int(
                                    config.get("talk", "sensevoice", "batch_size")
                                ),
                            )

                            def remove_angle_brackets_content(input_string: str):
                                # 使用正则表达式来匹配并删除 <> 之间的内容
                                return re.sub(r"<.*?>", "", input_string)

                            content = remove_angle_brackets_content(res[0]["text"])

                            talk_handle(content)
                    elif "google" == config.get("talk", "type"):
                        # 创建Recognizer对象
                        r = sr.Recognizer()

                        try:
                            # 打开麦克风进行录音
                            with sr.Microphone() as source:
                                logger.info("录音中...")
                                # 从麦克风获取音频数据
                                audio = r.listen(source)
                                logger.info("成功录制")

                                # 进行谷歌实时语音识别 en-US zh-CN ja-JP
                                content = r.recognize_google(
                                    audio,
                                    language=config.get("talk", "google", "tgt_lang"),
                                )

                                talk_handle(content)
                        except sr.UnknownValueError:
                            logger.warning("无法识别输入的语音")
                        except sr.RequestError as e:
                            logger.error("请求出错：" + str(e))

                    is_recording = False

                    if not status:
                        return
                except Exception as e:
                    logger.error(traceback.format_exc())
                    is_recording = False
                    return
        except Exception as e:
            logger.error(traceback.format_exc())
            is_recording = False
            return

    def on_key_press(event):
        global \
            do_listen_and_comment_thread, \
            stop_do_listen_and_comment_thread_event, \
            is_recording

        # 是否启用按键监听，不启用的话就不用执行了
        if not config.get("talk", "key_listener_enable"):
            return

        # if event.name in ['z', 'Z', 'c', 'C'] and keyboard.is_pressed('ctrl'):
        # logger.info("退出程序")

        # os._exit(0)

        # 按键CD
        current_time = time.time()
        if current_time - last_pressed < cooldown:
            return

        """
        触发按键部分的判断
        """
        trigger_key_lower = None
        stop_trigger_key_lower = None

        # trigger_key是字母, 整个小写
        if trigger_key.isalpha():
            trigger_key_lower = trigger_key.lower()

        # stop_trigger_key是字母, 整个小写
        if stop_trigger_key.isalpha():
            stop_trigger_key_lower = stop_trigger_key.lower()

        if trigger_key_lower:
            if event.name == trigger_key or event.name == trigger_key_lower:
                logger.info(f"检测到单击键盘 {event.name}，即将开始录音~")
            elif event.name == stop_trigger_key or event.name == stop_trigger_key_lower:
                logger.info(f"检测到单击键盘 {event.name}，即将停止录音~")
                stop_do_listen_and_comment_thread_event.set()
                return
            else:
                return
        else:
            if event.name == trigger_key:
                logger.info(f"检测到单击键盘 {event.name}，即将开始录音~")
            elif event.name == stop_trigger_key:
                logger.info(f"检测到单击键盘 {event.name}，即将停止录音~")
                stop_do_listen_and_comment_thread_event.set()
                return
            else:
                return

        if not is_recording:
            # 是否启用连续对话模式
            if config.get("talk", "continuous_talk"):
                stop_do_listen_and_comment_thread_event.clear()
                do_listen_and_comment_thread = threading.Thread(
                    target=do_listen_and_comment, args=(True,)
                )
                do_listen_and_comment_thread.start()
            else:
                stop_do_listen_and_comment_thread_event.clear()
                do_listen_and_comment_thread = threading.Thread(
                    target=do_listen_and_comment, args=(False,)
                )
                do_listen_and_comment_thread.start()
        else:
            logger.warning("正在录音中...请勿重复点击录音捏！")

    # 按键监听
    def key_listener():
        # 注册按键按下事件的回调函数
        keyboard.on_press(on_key_press)

        try:
            # 进入监听状态，等待按键按下
            keyboard.wait()
        except KeyboardInterrupt:
            os._exit(0)

    # 直接运行语音对话
    def direct_run_talk():
        global \
            do_listen_and_comment_thread, \
            stop_do_listen_and_comment_thread_event, \
            is_recording

        if not is_recording:
            # 是否启用连续对话模式
            if config.get("talk", "continuous_talk"):
                stop_do_listen_and_comment_thread_event.clear()
                do_listen_and_comment_thread = threading.Thread(
                    target=do_listen_and_comment, args=(True,)
                )
                do_listen_and_comment_thread.start()
            else:
                stop_do_listen_and_comment_thread_event.clear()
                do_listen_and_comment_thread = threading.Thread(
                    target=do_listen_and_comment, args=(False,)
                )
                do_listen_and_comment_thread.start()

    # 从配置文件中读取触发键的字符串配置
    trigger_key = config.get("talk", "trigger_key")
    stop_trigger_key = config.get("talk", "stop_trigger_key")

    # 是否启用了 按键监听
    if config.get("talk", "key_listener_enable"):
        logger.info(
            f"单击键盘 {trigger_key} 按键进行录音喵~ 由于其他任务还要启动，如果按键没有反应，请等待一段时间（如果使用本地ASR，请等待模型加载完成后使用）"
        )

    # 是否启用了直接运行对话，如果启用了，将在首次运行时直接进行语音识别，而不需手动点击开始按键。针对有些系统按键无法触发的情况下，配合连续对话和唤醒词使用
    if config.get("talk", "direct_run_talk"):
        logger.info("直接运行对话模式，首次运行时将直接进行语音识别，而不需手动点击开始按键（如果使用本地ASR，请等待模型加载完成后使用）")
        direct_run_talk()

    # 创建并启动按键监听线程，放着也是在聊天模式下，让程序一直阻塞用的
    thread = threading.Thread(target=key_listener)
    thread.start()

    # 定时任务
    def schedule_task(index):
        global config, common, my_handle, last_liveroom_data, last_username_list

        logger.debug("定时任务执行中...")
        hour, min = common.get_bj_time(6)

        if 0 <= hour and hour < 6:
            time = f"凌晨{hour}点{min}分"
        elif 6 <= hour and hour < 9:
            time = f"早晨{hour}点{min}分"
        elif 9 <= hour and hour < 12:
            time = f"上午{hour}点{min}分"
        elif hour == 12:
            time = f"中午{hour}点{min}分"
        elif 13 <= hour and hour < 18:
            time = f"下午{hour - 12}点{min}分"
        elif 18 <= hour and hour < 20:
            time = f"傍晚{hour - 12}点{min}分"
        elif 20 <= hour and hour < 24:
            time = f"晚上{hour - 12}点{min}分"

        # 根据对应索引从列表中随机获取一个值
        if len(config.get("schedule")[index]["copy"]) <= 0:
            return None

        random_copy = random.choice(config.get("schedule")[index]["copy"])

        # 假设有多个未知变量，用户可以在此处定义动态变量
        variables = {
            "time": time,
            "user_num": "N",
            "last_username": last_username_list[-1],
        }

        # 有用户数据情况的平台特殊处理
        if platform in ["dy", "tiktok"]:
            variables["user_num"] = last_liveroom_data["OnlineUserCount"]

        # 使用字典进行字符串替换
        if any(var in random_copy for var in variables):
            content = random_copy.format(
                **{var: value for var, value in variables.items() if var in random_copy}
            )
        else:
            content = random_copy

        content = common.brackets_text_randomize(content)

        data = {"platform": platform, "username": "定时任务", "content": content}

        logger.info(f"定时任务：{content}")

        my_handle.process_data(data, "schedule")

        # schedule.clear(index)

    # 启动定时任务
    def run_schedule():
        global config

        try:
            for index, task in enumerate(config.get("schedule")):
                if task["enable"]:
                    # logger.info(task)
                    min_seconds = int(task["time_min"])
                    max_seconds = int(task["time_max"])

                    def schedule_random_task(index, min_seconds, max_seconds):
                        schedule.clear(index)
                        # 在min_seconds和max_seconds之间随机选择下一次任务执行的时间
                        next_time = random.randint(min_seconds, max_seconds)
                        # logger.info(f"Next task {index} scheduled in {next_time} seconds at {time.ctime()}")

                        schedule_task(index)

                        schedule.every(next_time).seconds.do(
                            schedule_random_task, index, min_seconds, max_seconds
                        ).tag(index)

                    schedule_random_task(index, min_seconds, max_seconds)
        except Exception as e:
            logger.error(traceback.format_exc())

        while True:
            schedule.run_pending()
            # time.sleep(1)  # 控制每次循环的间隔时间，避免过多占用 CPU 资源

    # 创建定时任务子线程并启动 在平台是 dy的情况下，默认启动定时任务用于阻塞
    if any(item["enable"] for item in config.get("schedule")) or platform == "dy":
        # 创建定时任务子线程并启动
        schedule_thread = threading.Thread(target=run_schedule)
        schedule_thread.start()

    # 启动动态文案
    async def run_trends_copywriting():
        global config

        try:
            if not config.get("trends_copywriting", "enable"):
                return

            logger.info("动态文案任务线程运行中...")

            while True:
                # 文案文件路径列表
                copywriting_file_path_list = []

                # 获取动态文案列表
                for copywriting in config.get("trends_copywriting", "copywriting"):
                    # 获取文件夹内所有文件的文件绝对路径，包括文件扩展名
                    for tmp in common.get_all_file_paths(copywriting["folder_path"]):
                        copywriting_file_path_list.append(tmp)

                    # 是否开启随机播放
                    if config.get("trends_copywriting", "random_play"):
                        random.shuffle(copywriting_file_path_list)

                    logger.debug(
                        f"copywriting_file_path_list={copywriting_file_path_list}"
                    )

                    # 遍历文案文件路径列表
                    for copywriting_file_path in copywriting_file_path_list:
                        # 获取文案文件内容
                        copywriting_file_content = common.read_file_return_content(
                            copywriting_file_path
                        )
                        # 是否启用提示词对文案内容进行转换
                        if copywriting["prompt_change_enable"]:
                            data_json = {
                                "username": "trends_copywriting",
                                "content": copywriting["prompt_change_content"]
                                + copywriting_file_content,
                            }

                            # 调用函数进行LLM处理，以及生成回复内容，进行音频合成，需要好好考虑考虑实现
                            data_json["content"] = my_handle.llm_handle(
                                config.get("trends_copywriting", "llm_type"), data_json
                            )
                        else:
                            copywriting_file_content = common.brackets_text_randomize(
                                copywriting_file_content
                            )

                            data_json = {
                                "username": "trends_copywriting",
                                "content": copywriting_file_content,
                            }

                        logger.debug(
                            f'copywriting_file_content={copywriting_file_content},content={data_json["content"]}'
                        )

                        # 空数据判断
                        if (
                            data_json["content"] is not None
                            and data_json["content"] != ""
                        ):
                            # 发给直接复读进行处理
                            my_handle.reread_handle(
                                data_json, filter=True, type="trends_copywriting"
                            )

                            await asyncio.sleep(
                                config.get("trends_copywriting", "play_interval")
                            )
        except Exception as e:
            logger.error(traceback.format_exc())

    if config.get("trends_copywriting", "enable"):
        # 创建动态文案子线程并启动
        threading.Thread(target=lambda: asyncio.run(run_trends_copywriting())).start()

    # 闲时任务
    async def idle_time_task():
        global config, global_idle_time, common

        try:
            if not config.get("idle_time_task", "enable"):
                return

            logger.info("闲时任务线程运行中...")

            # 记录上一次触发的任务类型
            last_mode = 0
            copywriting_copy_list = None
            comment_copy_list = None
            local_audio_path_list = None

            overflow_time_min = int(config.get("idle_time_task", "idle_time_min"))
            overflow_time_max = int(config.get("idle_time_task", "idle_time_max"))
            overflow_time = random.randint(overflow_time_min, overflow_time_max)

            logger.info(f"下一个闲时任务将在{overflow_time}秒后执行")

            def load_data_list(type):
                if type == "copywriting":
                    tmp = config.get("idle_time_task", "copywriting", "copy")
                elif type == "comment":
                    tmp = config.get("idle_time_task", "comment", "copy")
                elif type == "local_audio":
                    tmp = config.get("idle_time_task", "local_audio", "path")

                logger.debug(f"type={type}, tmp={tmp}")
                tmp2 = copy.copy(tmp)
                return tmp2

            # 加载数据到list
            copywriting_copy_list = load_data_list("copywriting")
            comment_copy_list = load_data_list("comment")
            local_audio_path_list = load_data_list("local_audio")

            logger.debug(f"copywriting_copy_list={copywriting_copy_list}")
            logger.debug(f"comment_copy_list={comment_copy_list}")
            logger.debug(f"local_audio_path_list={local_audio_path_list}")

            def do_task(
                last_mode,
                copywriting_copy_list,
                comment_copy_list,
                local_audio_path_list,
            ):
                global global_idle_time

                # 闲时计数清零
                global_idle_time = 0

                # 闲时任务处理
                if config.get("idle_time_task", "copywriting", "enable"):
                    if last_mode == 0:
                        # 是否开启了随机触发
                        if config.get("idle_time_task", "copywriting", "random"):
                            logger.debug("切换到文案触发模式")
                            if copywriting_copy_list != []:
                                # 随机打乱列表中的元素
                                random.shuffle(copywriting_copy_list)
                                copywriting_copy = copywriting_copy_list.pop(0)
                            else:
                                # 刷新list数据
                                copywriting_copy_list = load_data_list("copywriting")
                                # 随机打乱列表中的元素
                                random.shuffle(copywriting_copy_list)
                                if copywriting_copy_list != []:
                                    copywriting_copy = copywriting_copy_list.pop(0)
                                else:
                                    return (
                                        last_mode,
                                        copywriting_copy_list,
                                        comment_copy_list,
                                        local_audio_path_list,
                                    )
                        else:
                            logger.debug(copywriting_copy_list)
                            if copywriting_copy_list != []:
                                copywriting_copy = copywriting_copy_list.pop(0)
                            else:
                                # 刷新list数据
                                copywriting_copy_list = load_data_list("copywriting")
                                if copywriting_copy_list != []:
                                    copywriting_copy = copywriting_copy_list.pop(0)
                                else:
                                    return (
                                        last_mode,
                                        copywriting_copy_list,
                                        comment_copy_list,
                                        local_audio_path_list,
                                    )

                        hour, min = common.get_bj_time(6)

                        if 0 <= hour and hour < 6:
                            time = f"凌晨{hour}点{min}分"
                        elif 6 <= hour and hour < 9:
                            time = f"早晨{hour}点{min}分"
                        elif 9 <= hour and hour < 12:
                            time = f"上午{hour}点{min}分"
                        elif hour == 12:
                            time = f"中午{hour}点{min}分"
                        elif 13 <= hour and hour < 18:
                            time = f"下午{hour - 12}点{min}分"
                        elif 18 <= hour and hour < 20:
                            time = f"傍晚{hour - 12}点{min}分"
                        elif 20 <= hour and hour < 24:
                            time = f"晚上{hour - 12}点{min}分"

                        # 动态变量替换
                        # 假设有多个未知变量，用户可以在此处定义动态变量
                        variables = {
                            "time": time,
                            "user_num": "N",
                            "last_username": last_username_list[-1],
                        }

                        # 有用户数据情况的平台特殊处理
                        if platform in ["dy", "tiktok"]:
                            variables["user_num"] = last_liveroom_data[
                                "OnlineUserCount"
                            ]

                        # 使用字典进行字符串替换
                        if any(var in copywriting_copy for var in variables):
                            copywriting_copy = copywriting_copy.format(
                                **{
                                    var: value
                                    for var, value in variables.items()
                                    if var in copywriting_copy
                                }
                            )

                        # [1|2]括号语法随机获取一个值，返回取值完成后的字符串
                        copywriting_copy = common.brackets_text_randomize(
                            copywriting_copy
                        )

                        # 发送给处理函数
                        data = {
                            "platform": platform,
                            "username": "闲时任务-文案模式",
                            "type": "reread",
                            "content": copywriting_copy,
                        }

                        my_handle.process_data(data, "idle_time_task")

                        # 模式切换
                        last_mode = 1

                        overflow_time = random.randint(
                            overflow_time_min, overflow_time_max
                        )
                        logger.info(f"下一个闲时任务将在{overflow_time}秒后执行")

                        return (
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        )
                else:
                    last_mode = 1

                if config.get("idle_time_task", "comment", "enable"):
                    if last_mode == 1:
                        # 是否开启了随机触发
                        if config.get("idle_time_task", "comment", "random"):
                            logger.debug("切换到弹幕触发LLM模式")
                            if comment_copy_list != []:
                                # 随机打乱列表中的元素
                                random.shuffle(comment_copy_list)
                                comment_copy = comment_copy_list.pop(0)
                            else:
                                # 刷新list数据
                                comment_copy_list = load_data_list("comment")
                                # 随机打乱列表中的元素
                                random.shuffle(comment_copy_list)
                                comment_copy = comment_copy_list.pop(0)
                        else:
                            if comment_copy_list != []:
                                comment_copy = comment_copy_list.pop(0)
                            else:
                                # 刷新list数据
                                comment_copy_list = load_data_list("comment")
                                comment_copy = comment_copy_list.pop(0)

                        hour, min = common.get_bj_time(6)

                        if 0 <= hour and hour < 6:
                            time = f"凌晨{hour}点{min}分"
                        elif 6 <= hour and hour < 9:
                            time = f"早晨{hour}点{min}分"
                        elif 9 <= hour and hour < 12:
                            time = f"上午{hour}点{min}分"
                        elif hour == 12:
                            time = f"中午{hour}点{min}分"
                        elif 13 <= hour and hour < 18:
                            time = f"下午{hour - 12}点{min}分"
                        elif 18 <= hour and hour < 20:
                            time = f"傍晚{hour - 12}点{min}分"
                        elif 20 <= hour and hour < 24:
                            time = f"晚上{hour - 12}点{min}分"

                        # 动态变量替换
                        # 假设有多个未知变量，用户可以在此处定义动态变量
                        variables = {
                            "time": time,
                            "user_num": "N",
                            "last_username": last_username_list[-1],
                        }

                        # 有用户数据情况的平台特殊处理
                        if platform in ["dy", "tiktok"]:
                            variables["user_num"] = last_liveroom_data[
                                "OnlineUserCount"
                            ]

                        # 使用字典进行字符串替换
                        if any(var in comment_copy for var in variables):
                            comment_copy = comment_copy.format(
                                **{
                                    var: value
                                    for var, value in variables.items()
                                    if var in comment_copy
                                }
                            )

                        # [1|2]括号语法随机获取一个值，返回取值完成后的字符串
                        comment_copy = common.brackets_text_randomize(comment_copy)

                        # 发送给处理函数
                        data = {
                            "platform": platform,
                            "username": "闲时任务-弹幕触发LLM模式",
                            "type": "comment",
                            "content": comment_copy,
                        }

                        my_handle.process_data(data, "idle_time_task")

                        # 模式切换
                        last_mode = 2

                        overflow_time = random.randint(
                            overflow_time_min, overflow_time_max
                        )
                        logger.info(f"下一个闲时任务将在{overflow_time}秒后执行")

                        return (
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        )
                else:
                    last_mode = 2

                if config.get("idle_time_task", "local_audio", "enable"):
                    if last_mode == 2:
                        logger.debug("切换到本地音频模式")

                        # 是否开启了随机触发
                        if config.get("idle_time_task", "local_audio", "random"):
                            if local_audio_path_list != []:
                                # 随机打乱列表中的元素
                                random.shuffle(local_audio_path_list)
                                local_audio_path = local_audio_path_list.pop(0)
                            else:
                                # 刷新list数据
                                local_audio_path_list = load_data_list("local_audio")
                                # 随机打乱列表中的元素
                                random.shuffle(local_audio_path_list)
                                local_audio_path = local_audio_path_list.pop(0)
                        else:
                            if local_audio_path_list != []:
                                local_audio_path = local_audio_path_list.pop(0)
                            else:
                                # 刷新list数据
                                local_audio_path_list = load_data_list("local_audio")
                                local_audio_path = local_audio_path_list.pop(0)

                        # [1|2]括号语法随机获取一个值，返回取值完成后的字符串
                        local_audio_path = common.brackets_text_randomize(
                            local_audio_path
                        )

                        logger.debug(f"local_audio_path={local_audio_path}")

                        # 发送给处理函数
                        data = {
                            "platform": platform,
                            "username": "闲时任务-本地音频模式",
                            "type": "local_audio",
                            "content": common.extract_filename(local_audio_path, False),
                            "file_path": local_audio_path,
                        }

                        my_handle.process_data(data, "idle_time_task")

                        # 模式切换
                        last_mode = 0

                        overflow_time = random.randint(
                            overflow_time_min, overflow_time_max
                        )
                        logger.info(f"下一个闲时任务将在{overflow_time}秒后执行")

                        return (
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        )
                else:
                    last_mode = 0

                return (
                    last_mode,
                    copywriting_copy_list,
                    comment_copy_list,
                    local_audio_path_list,
                )

            while True:
                # 如果闲时时间范围为0，就睡眠100ms 意思意思
                if overflow_time_min > 0 and overflow_time_min > 0:
                    # 每隔一秒的睡眠进行闲时计数
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(0.1)
                global_idle_time = global_idle_time + 1

                if config.get("idle_time_task", "type") == "直播间无消息更新闲时":
                    # 闲时计数达到指定值，进行闲时任务处理
                    if global_idle_time >= overflow_time:
                        (
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        ) = do_task(
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        )
                elif config.get("idle_time_task", "type") == "待合成消息队列更新闲时":
                    if my_handle.is_queue_less_or_greater_than(
                        type="message_queue",
                        less=int(
                            config.get("idle_time_task", "min_msg_queue_len_to_trigger")
                        ),
                    ):
                        (
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        ) = do_task(
                            last_mode,
                            copywriting_copy_list,
                            comment_copy_list,
                            local_audio_path_list,
                        )
                elif config.get("idle_time_task", "type") == "待播放音频队列更新闲时":
                    logger.debug(f"待播放音频数：{wait_play_audio_num}")
                    # 特殊处理：metahuman_stream平台，判断wait_play_audio_num
                    if config.get("visual_body") == "metahuman_stream":
                        if wait_play_audio_num < config.get("idle_time_task", "min_audio_queue_len_to_trigger"):
                            (
                                last_mode,
                                copywriting_copy_list,
                                comment_copy_list,
                                local_audio_path_list,
                            ) = do_task(
                                last_mode,
                                copywriting_copy_list,
                                comment_copy_list,
                                local_audio_path_list,
                            )
                    else:
                        if my_handle.is_queue_less_or_greater_than(
                            type="voice_tmp_path_queue",
                            less=int(
                                config.get(
                                    "idle_time_task", "min_audio_queue_len_to_trigger"
                                )
                            ),
                        ):
                            (
                                last_mode,
                                copywriting_copy_list,
                                comment_copy_list,
                                local_audio_path_list,
                            ) = do_task(
                                last_mode,
                                copywriting_copy_list,
                                comment_copy_list,
                                local_audio_path_list,
                            )

        except Exception as e:
            logger.error(traceback.format_exc())

    if config.get("idle_time_task", "enable"):
        # 创建闲时任务子线程并启动
        threading.Thread(target=lambda: asyncio.run(idle_time_task())).start()

    # 闲时任务计时自动清零
    def idle_time_auto_clear(type: str):
        """闲时任务计时自动清零

        Args:
            type (str): 消息类型（comment/gift/entrance等）

        Returns:
            bool: 是否清零的结果
        """
        global config, global_idle_time

        # 触发的类型列表
        type_list = config.get("idle_time_task", "trigger_type")
        if type in type_list:
            global_idle_time = 0

            return True

        return False

    # 图像识别 定时任务
    def image_recognition_schedule_task(type: str):
        global config, common, my_handle

        logger.debug(f"图像识别-{type} 定时任务执行中...")

        data = {"platform": platform, "username": None, "content": "", "type": type}

        logger.info(f"图像识别-{type} 定时任务触发")

        my_handle.process_data(data, "image_recognition_schedule")

    # 启动图像识别 定时任务
    def run_image_recognition_schedule(interval: int, type: str):
        global config

        try:
            schedule.every(interval).seconds.do(
                partial(image_recognition_schedule_task, type)
            )
        except Exception as e:
            logger.error(traceback.format_exc())

        while True:
            schedule.run_pending()
            # time.sleep(1)  # 控制每次循环的间隔时间，避免过多占用 CPU 资源

    if config.get("image_recognition", "loop_screenshot_enable"):
        # 创建定时任务子线程并启动
        image_recognition_schedule_thread = threading.Thread(
            target=lambda: run_image_recognition_schedule(
                config.get("image_recognition", "loop_screenshot_delay"), "窗口截图"
            )
        )
        image_recognition_schedule_thread.start()

    if config.get("image_recognition", "loop_cam_screenshot_enable"):
        # 创建定时任务子线程并启动
        image_recognition_cam_schedule_thread = threading.Thread(
            target=lambda: run_image_recognition_schedule(
                config.get("image_recognition", "loop_cam_screenshot_delay"),
                "摄像头截图",
            )
        )
        image_recognition_cam_schedule_thread.start()

    # 针对对接LiveTalking(metahuman-stream)特殊处理
    if config.get("visual_body") == "metahuman_stream":
        def metahuman_stream_is_speaking():
            global wait_play_audio_num

            try:
                from urllib.parse import urljoin
                url = urljoin(
                    config.get("metahuman_stream", "api_ip_port"), "is_speaking"
                )
                resp_json = common.send_request(url, 'POST', {"sessionid": 0}, timeout=5)
                if resp_json and resp_json["code"] == 0:
                    if resp_json["data"]:
                        logger.debug("LiveTalking有音频在播放")
                        wait_play_audio_num = 1
                    else:
                        logger.debug("LiveTalking没有音频在播放")
                        wait_play_audio_num = 0
                        
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("请求LiveTalking is_speaking接口失败")

        # 创建线程定时请求LiveTalking的is_speaking接口，判断是否有音频在播放
        def run_metahuman_stream_is_speaking_schedule():
            interval = 3
            try:
                schedule.every(interval).seconds.do(
                    partial(metahuman_stream_is_speaking)
                )
            except Exception as e:
                logger.error(traceback.format_exc())

            while True:
                schedule.run_pending()    

        run_metahuman_stream_is_speaking_schedule_thread = threading.Thread(
            target=lambda: run_metahuman_stream_is_speaking_schedule()
        )
        run_metahuman_stream_is_speaking_schedule_thread.start()
    
    logger.info(f"当前平台：{platform}")

    if platform == "bilibili":
        from bilibili_api import Credential, live, sync, login

        try:
            if config.get("bilibili", "login_type") == "cookie":
                logger.info(
                    "b站登录后F12抓网络包获取cookie，强烈建议使用小号！有封号风险"
                )
                logger.info(
                    "b站登录后，F12控制台，输入 window.localStorage.ac_time_value 回车获取(如果没有，请重新登录)"
                )

                bilibili_cookie = config.get("bilibili", "cookie")
                bilibili_ac_time_value = config.get("bilibili", "ac_time_value")
                if bilibili_ac_time_value == "":
                    bilibili_ac_time_value = None

                # logger.info(f'SESSDATA={common.parse_cookie_data(bilibili_cookie, "SESSDATA")}')
                # logger.info(f'bili_jct={common.parse_cookie_data(bilibili_cookie, "bili_jct")}')
                # logger.info(f'buvid3={common.parse_cookie_data(bilibili_cookie, "buvid3")}')
                # logger.info(f'DedeUserID={common.parse_cookie_data(bilibili_cookie, "DedeUserID")}')

                # 生成一个 Credential 对象
                credential = Credential(
                    sessdata=common.parse_cookie_data(bilibili_cookie, "SESSDATA"),
                    bili_jct=common.parse_cookie_data(bilibili_cookie, "bili_jct"),
                    buvid3=common.parse_cookie_data(bilibili_cookie, "buvid3"),
                    dedeuserid=common.parse_cookie_data(bilibili_cookie, "DedeUserID"),
                    ac_time_value=bilibili_ac_time_value,
                )
            elif config.get("bilibili", "login_type") == "手机扫码":
                credential = login.login_with_qrcode()
            elif config.get("bilibili", "login_type") == "手机扫码-终端":
                credential = login.login_with_qrcode_term()
            elif config.get("bilibili", "login_type") == "账号密码登录":
                bilibili_username = config.get("bilibili", "username")
                bilibili_password = config.get("bilibili", "password")

                credential = login.login_with_password(
                    bilibili_username, bilibili_password
                )
            elif config.get("bilibili", "login_type") == "不登录":
                credential = None
            else:
                credential = login.login_with_qrcode()

            # 初始化 Bilibili 直播间
            room = live.LiveDanmaku(my_handle.get_room_id(), credential=credential)
        except Exception as e:
            logger.error(traceback.format_exc())
            my_handle.abnormal_alarm_handle("platform")
            # os._exit(0)

        """
        DANMU_MSG: 用户发送弹幕
        SEND_GIFT: 礼物
        COMBO_SEND：礼物连击
        GUARD_BUY：续费大航海
        SUPER_CHAT_MESSAGE：醒目留言（SC）
        SUPER_CHAT_MESSAGE_JPN：醒目留言（带日语翻译？）
        WELCOME: 老爷进入房间
        WELCOME_GUARD: 房管进入房间
        NOTICE_MSG: 系统通知（全频道广播之类的）
        PREPARING: 直播准备中
        LIVE: 直播开始
        ROOM_REAL_TIME_MESSAGE_UPDATE: 粉丝数等更新
        ENTRY_EFFECT: 进场特效
        ROOM_RANK: 房间排名更新
        INTERACT_WORD: 用户进入直播间
        ACTIVITY_BANNER_UPDATE_V2: 好像是房间名旁边那个xx小时榜
        本模块自定义事件：
        VIEW: 直播间人气更新
        ALL: 所有事件
        DISCONNECT: 断开连接（传入连接状态码参数）
        TIMEOUT: 心跳响应超时
        VERIFICATION_SUCCESSFUL: 认证成功
        """

        @room.on("DANMU_MSG")
        async def _(event):
            """
            处理直播间弹幕事件
            :param event: 弹幕事件数据
            """

            # 闲时计数清零
            idle_time_auto_clear("comment")

            content = event["data"]["info"][1]  # 获取弹幕内容
            username = event["data"]["info"][2][1]  # 获取发送弹幕的用户昵称

            logger.info(f"[{username}]: {content}")

            data = {"platform": platform, "username": username, "content": content}

            my_handle.process_data(data, "comment")

        @room.on("COMBO_SEND")
        async def _(event):
            """
            处理直播间礼物连击事件
            :param event: 礼物连击事件数据
            """
            idle_time_auto_clear("gift")

            gift_name = event["data"]["data"]["gift_name"]
            username = event["data"]["data"]["uname"]
            # 礼物数量
            combo_num = event["data"]["data"]["combo_num"]
            # 总金额
            combo_total_coin = event["data"]["data"]["combo_total_coin"]

            logger.info(
                f"用户：{username} 赠送 {combo_num} 个 {gift_name}，总计 {combo_total_coin}电池"
            )

            data = {
                "platform": platform,
                "gift_name": gift_name,
                "username": username,
                "num": combo_num,
                "unit_price": combo_total_coin / combo_num / 1000,
                "total_price": combo_total_coin / 1000,
            }

            my_handle.process_data(data, "gift")

        @room.on("SEND_GIFT")
        async def _(event):
            """
            处理直播间礼物事件
            :param event: 礼物事件数据
            """
            idle_time_auto_clear("gift")

            # logger.info(event)

            gift_name = event["data"]["data"]["giftName"]
            username = event["data"]["data"]["uname"]
            # 礼物数量
            num = event["data"]["data"]["num"]
            # 总金额
            combo_total_coin = event["data"]["data"]["combo_total_coin"]
            # 单个礼物金额
            discount_price = event["data"]["data"]["discount_price"]

            logger.info(
                f"用户：{username} 赠送 {num} 个 {gift_name}，单价 {discount_price}电池，总计 {combo_total_coin}电池"
            )

            data = {
                "platform": platform,
                "gift_name": gift_name,
                "username": username,
                "num": num,
                "unit_price": discount_price / 1000,
                "total_price": combo_total_coin / 1000,
            }

            my_handle.process_data(data, "gift")

        @room.on("GUARD_BUY")
        async def _(event):
            """
            处理直播间续费大航海事件
            :param event: 续费大航海事件数据
            """

            logger.info(event)

        @room.on("SUPER_CHAT_MESSAGE")
        async def _(event):
            """
            处理直播间醒目留言（SC）事件
            :param event: 醒目留言（SC）事件数据
            """
            idle_time_auto_clear("gift")

            message = event["data"]["data"]["message"]
            uname = event["data"]["data"]["user_info"]["uname"]
            price = event["data"]["data"]["price"]

            logger.info(f"用户：{uname} 发送 {price}元 SC：{message}")

            data = {
                "platform": platform,
                "gift_name": "SC",
                "username": uname,
                "num": 1,
                "unit_price": price,
                "total_price": price,
                "content": message,
            }

            my_handle.process_data(data, "gift")

            my_handle.process_data(data, "comment")

        @room.on("INTERACT_WORD")
        async def _(event):
            """
            处理直播间用户进入直播间事件
            :param event: 用户进入直播间事件数据
            """
            global last_username_list

            idle_time_auto_clear("entrance")

            username = event["data"]["data"]["uname"]

            logger.info(f"用户：{username} 进入直播间")

            # 添加用户名到最新的用户名列表
            add_username_to_last_username_list(username)

            data = {"platform": platform, "username": username, "content": "进入直播间"}

            my_handle.process_data(data, "entrance")

        # @room.on('WELCOME')
        # async def _(event):
        #     """
        #     处理直播间老爷进入房间事件
        #     :param event: 老爷进入房间事件数据
        #     """

        #     logger.info(event)

        # @room.on('WELCOME_GUARD')
        # async def _(event):
        #     """
        #     处理直播间房管进入房间事件
        #     :param event: 房管进入房间事件数据
        #     """

        #     logger.info(event)

        try:
            # 启动 Bilibili 直播间连接
            sync(room.connect())
        except KeyboardInterrupt:
            logger.warning("程序被强行退出")
        finally:
            logger.warning("关闭连接...可能是直播间号配置有误或者其他原因导致的")
            os._exit(0)
    elif platform == "bilibili2":
        import blivedm
        import blivedm.models.web as web_models
        import blivedm.models.open_live as open_models

        global SESSDATA

        # 直播间ID的取值看直播间URL
        TEST_ROOM_IDS = [my_handle.get_room_id()]

        try:
            if config.get("bilibili", "login_type") == "cookie":
                bilibili_cookie = config.get("bilibili", "cookie")
                SESSDATA = common.parse_cookie_data(bilibili_cookie, "SESSDATA")
            elif config.get("bilibili", "login_type") == "open_live":
                # 在开放平台申请的开发者密钥 https://open-live.bilibili.com/open-manage
                ACCESS_KEY_ID = config.get("bilibili", "open_live", "ACCESS_KEY_ID")
                ACCESS_KEY_SECRET = config.get(
                    "bilibili", "open_live", "ACCESS_KEY_SECRET"
                )
                # 在开放平台创建的项目ID
                APP_ID = config.get("bilibili", "open_live", "APP_ID")
                # 主播身份码 直播中心获取
                ROOM_OWNER_AUTH_CODE = config.get(
                    "bilibili", "open_live", "ROOM_OWNER_AUTH_CODE"
                )

        except Exception as e:
            logger.error(traceback.format_exc())
            my_handle.abnormal_alarm_handle("platform")

        async def main_func():
            global session

            if config.get("bilibili", "login_type") == "open_live":
                await run_single_client2()
            else:
                try:
                    init_session()

                    await run_single_client()
                    await run_multi_clients()
                finally:
                    await session.close()

        def init_session():
            global session, SESSDATA

            cookies = http.cookies.SimpleCookie()
            cookies["SESSDATA"] = SESSDATA
            cookies["SESSDATA"]["domain"] = "bilibili.com"

            # logger.info(f"SESSDATA={SESSDATA}")

            # logger.warning(f"sessdata={SESSDATA}")
            # logger.warning(f"cookies={cookies}")

            session = aiohttp.ClientSession()
            session.cookie_jar.update_cookies(cookies)

        async def run_single_client():
            """
            演示监听一个直播间
            """
            global session

            room_id = random.choice(TEST_ROOM_IDS)
            client = blivedm.BLiveClient(room_id, session=session)
            handler = MyHandler()
            client.set_handler(handler)

            client.start()
            try:
                # 演示5秒后停止
                await asyncio.sleep(5)
                client.stop()

                await client.join()
            finally:
                await client.stop_and_close()

        async def run_single_client2():
            """
            演示监听一个直播间 开放平台
            """
            client = blivedm.OpenLiveClient(
                access_key_id=ACCESS_KEY_ID,
                access_key_secret=ACCESS_KEY_SECRET,
                app_id=APP_ID,
                room_owner_auth_code=ROOM_OWNER_AUTH_CODE,
            )
            handler = MyHandler2()
            client.set_handler(handler)

            client.start()
            try:
                # 演示70秒后停止
                # await asyncio.sleep(70)
                # client.stop()

                await client.join()
            finally:
                await client.stop_and_close()

        async def run_multi_clients():
            """
            演示同时监听多个直播间
            """
            global session

            clients = [
                blivedm.BLiveClient(room_id, session=session)
                for room_id in TEST_ROOM_IDS
            ]
            handler = MyHandler()
            for client in clients:
                client.set_handler(handler)
                client.start()

            try:
                await asyncio.gather(*(client.join() for client in clients))
            finally:
                await asyncio.gather(*(client.stop_and_close() for client in clients))

        class MyHandler(blivedm.BaseHandler):
            # 演示如何添加自定义回调
            _CMD_CALLBACK_DICT = blivedm.BaseHandler._CMD_CALLBACK_DICT.copy()

            # 入场消息回调
            def __interact_word_callback(
                self, client: blivedm.BLiveClient, command: dict
            ):
                # logger.info(f"[{client.room_id}] INTERACT_WORD: self_type={type(self).__name__}, room_id={client.room_id},"
                #     f" uname={command['data']['uname']}")

                global last_username_list

                idle_time_auto_clear("entrance")

                username = command["data"]["uname"]

                logger.info(f"用户：{username} 进入直播间")

                # 添加用户名到最新的用户名列表
                add_username_to_last_username_list(username)

                data = {
                    "platform": platform,
                    "username": username,
                    "content": "进入直播间",
                }

                my_handle.process_data(data, "entrance")

            _CMD_CALLBACK_DICT["INTERACT_WORD"] = __interact_word_callback  # noqa

            def _on_heartbeat(
                self, client: blivedm.BLiveClient, message: web_models.HeartbeatMessage
            ):
                logger.debug(f"[{client.room_id}] 心跳")

            def _on_danmaku(
                self, client: blivedm.BLiveClient, message: web_models.DanmakuMessage
            ):
                # 闲时计数清零
                idle_time_auto_clear("comment")

                # logger.info(f'[{client.room_id}] {message.uname}：{message.msg}')
                content = message.msg  # 获取弹幕内容
                username = message.uname  # 获取发送弹幕的用户昵称
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None

                logger.info(f"[{username}]: {content}")

                data = {
                    "platform": platform,
                    "username": username,
                    "user_face": user_face,
                    "content": content,
                }

                my_handle.process_data(data, "comment")

            def _on_gift(
                self, client: blivedm.BLiveClient, message: web_models.GiftMessage
            ):
                # logger.info(f'[{client.room_id}] {message.uname} 赠送{message.gift_name}x{message.num}'
                #     f' （{message.coin_type}瓜子x{message.total_coin}）')
                idle_time_auto_clear("gift")

                gift_name = message.gift_name
                username = message.uname
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None

                # 礼物数量
                combo_num = message.num
                # 总金额
                combo_total_coin = message.total_coin

                logger.info(
                    f"用户：{username} 赠送 {combo_num} 个 {gift_name}，总计 {combo_total_coin}电池"
                )

                data = {
                    "platform": platform,
                    "gift_name": gift_name,
                    "username": username,
                    "user_face": user_face,
                    "num": combo_num,
                    "unit_price": combo_total_coin / combo_num / 1000,
                    "total_price": combo_total_coin / 1000,
                }

                my_handle.process_data(data, "gift")

            def _on_buy_guard(
                self, client: blivedm.BLiveClient, message: web_models.GuardBuyMessage
            ):
                logger.info(
                    f"[{client.room_id}] {message.username} 购买{message.gift_name}"
                )

            def _on_super_chat(
                self, client: blivedm.BLiveClient, message: web_models.SuperChatMessage
            ):
                # logger.info(f'[{client.room_id}] 醒目留言 ¥{message.price} {message.uname}：{message.message}')
                idle_time_auto_clear("gift")

                message = message.message
                uname = message.uname
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None
                price = message.price

                logger.info(f"用户：{uname} 发送 {price}元 SC：{message}")

                data = {
                    "platform": platform,
                    "gift_name": "SC",
                    "username": uname,
                    "user_face": user_face,
                    "num": 1,
                    "unit_price": price,
                    "total_price": price,
                    "content": message,
                }

                my_handle.process_data(data, "gift")

                my_handle.process_data(data, "comment")

        class MyHandler2(blivedm.BaseHandler):
            def _on_heartbeat(
                self, client: blivedm.BLiveClient, message: web_models.HeartbeatMessage
            ):
                logger.debug(f"[{client.room_id}] 心跳")

            def _on_open_live_danmaku(
                self,
                client: blivedm.OpenLiveClient,
                message: open_models.DanmakuMessage,
            ):
                # 闲时计数清零
                idle_time_auto_clear("comment")

                # logger.info(f'[{client.room_id}] {message.uname}：{message.msg}')
                content = message.msg  # 获取弹幕内容
                username = message.uname  # 获取发送弹幕的用户昵称
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None

                logger.debug(f"用户：{username} 头像：{user_face}")

                logger.info(f"[{username}]: {content}")

                data = {
                    "platform": platform,
                    "username": username,
                    "user_face": user_face,
                    "content": content,
                }

                my_handle.process_data(data, "comment")

            def _on_open_live_gift(
                self, client: blivedm.OpenLiveClient, message: open_models.GiftMessage
            ):
                idle_time_auto_clear("gift")

                gift_name = message.gift_name
                username = message.uname
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None
                # 礼物数量
                combo_num = message.gift_num
                # 总金额
                combo_total_coin = message.price * message.gift_num

                logger.info(
                    f"用户：{username} 赠送 {combo_num} 个 {gift_name}，总计 {combo_total_coin}电池"
                )

                data = {
                    "platform": platform,
                    "gift_name": gift_name,
                    "username": username,
                    "user_face": user_face,
                    "num": combo_num,
                    "unit_price": combo_total_coin / combo_num / 1000,
                    "total_price": combo_total_coin / 1000,
                }

                my_handle.process_data(data, "gift")

            def _on_open_live_buy_guard(
                self,
                client: blivedm.OpenLiveClient,
                message: open_models.GuardBuyMessage,
            ):
                logger.info(
                    f"[{client.room_id}] {message.user_info.uname} 购买 大航海等级={message.guard_level}"
                )

            def _on_open_live_super_chat(
                self,
                client: blivedm.OpenLiveClient,
                message: open_models.SuperChatMessage,
            ):
                idle_time_auto_clear("gift")

                logger.info(
                    f"[{message.room_id}] 醒目留言 ¥{message.rmb} {message.uname}：{message.message}"
                )

                message = message.message
                uname = message.uname
                # 检查是否存在 face 属性
                user_face = message.face if hasattr(message, "face") else None
                price = message.rmb

                logger.info(f"用户：{uname} 发送 {price}元 SC：{message}")

                data = {
                    "platform": platform,
                    "gift_name": "SC",
                    "username": uname,
                    "user_face": user_face,
                    "num": 1,
                    "unit_price": price,
                    "total_price": price,
                    "content": message,
                }

                my_handle.process_data(data, "gift")

                my_handle.process_data(data, "comment")

            def _on_open_live_super_chat_delete(
                self,
                client: blivedm.OpenLiveClient,
                message: open_models.SuperChatDeleteMessage,
            ):
                logger.info(
                    f"[直播间 {message.room_id}] 删除醒目留言 message_ids={message.message_ids}"
                )

            def _on_open_live_like(
                self, client: blivedm.OpenLiveClient, message: open_models.LikeMessage
            ):
                logger.info(f"用户：{message.uname} 点了个赞")

        asyncio.run(main_func())
    elif platform == "dy":
        import websocket

        def on_message(ws, message):
            global last_liveroom_data, last_username_list, config, config_path
            global global_idle_time

            message_json = json.loads(message)
            # logger.debug(message_json)
            if "Type" in message_json:
                type = message_json["Type"]
                data_json = json.loads(message_json["Data"])

                if type == 1:
                    # 闲时计数清零
                    idle_time_auto_clear("comment")

                    username = data_json["User"]["Nickname"]
                    content = data_json["Content"]

                    logger.info(f"[📧直播间弹幕消息] [{username}]：{content}")

                    data = {
                        "platform": platform,
                        "username": username,
                        "content": content,
                    }

                    my_handle.process_data(data, "comment")

                    pass

                elif type == 2:
                    username = data_json["User"]["Nickname"]
                    count = data_json["Count"]

                    logger.info(f"[👍直播间点赞消息] {username} 点了{count}赞")

                elif type == 3:
                    idle_time_auto_clear("entrance")

                    username = data_json["User"]["Nickname"]

                    logger.info(f"[🚹🚺直播间成员加入消息] 欢迎 {username} 进入直播间")

                    data = {
                        "platform": platform,
                        "username": username,
                        "content": "进入直播间",
                    }

                    # 添加用户名到最新的用户名列表
                    add_username_to_last_username_list(username)

                    my_handle.process_data(data, "entrance")

                elif type == 4:
                    idle_time_auto_clear("follow")

                    username = data_json["User"]["Nickname"]

                    logger.info(
                        f'[➕直播间关注消息] 感谢 {data_json["User"]["Nickname"]} 的关注'
                    )

                    data = {"platform": platform, "username": username}

                    my_handle.process_data(data, "follow")

                    pass

                elif type == 5:
                    idle_time_auto_clear("gift")

                    gift_name = data_json["GiftName"]
                    username = data_json["User"]["Nickname"]
                    # 礼物数量
                    num = data_json["GiftCount"]
                    # 礼物重复数量
                    repeat_count = data_json["RepeatCount"]

                    try:
                        # 暂时是写死的
                        data_path = "data/抖音礼物价格表.json"

                        # 读取JSON文件
                        with open(data_path, "r", encoding="utf-8") as file:
                            # 解析JSON数据
                            data_json = json.load(file)

                        if gift_name in data_json:
                            # 单个礼物金额 需要自己维护礼物价值表
                            discount_price = data_json[gift_name]
                        else:
                            logger.warning(
                                f"数据文件：{data_path} 中，没有 {gift_name} 对应的价值，请手动补充数据"
                            )
                            discount_price = 1
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        discount_price = 1

                    # 总金额
                    combo_total_coin = repeat_count * discount_price

                    logger.info(
                        f"[🎁直播间礼物消息] 用户：{username} 赠送 {num} 个 {gift_name}，单价 {discount_price}抖币，总计 {combo_total_coin}抖币"
                    )

                    data = {
                        "platform": platform,
                        "gift_name": gift_name,
                        "username": username,
                        "num": num,
                        "unit_price": discount_price / 10,
                        "total_price": combo_total_coin / 10,
                    }

                    my_handle.process_data(data, "gift")

                elif type == 6:
                    logger.info(f'[直播间数据] {data_json["Content"]}')
                    # {'OnlineUserCount': 50, 'TotalUserCount': 22003, 'TotalUserCountStr': '2.2万', 'OnlineUserCountStr': '50',
                    # 'MsgId': 7260517442466662207, 'User': None, 'Content': '当前直播间人数 50，累计直播间人数 2.2万', 'RoomId': 7260415920948906807}
                    # logger.info(f"data_json={data_json}")

                    last_liveroom_data = data_json

                    # 当前在线人数
                    OnlineUserCount = data_json["OnlineUserCount"]

                    try:
                        # 是否开启了动态配置功能
                        if config.get("trends_config", "enable"):
                            for path_config in config.get("trends_config", "path"):
                                online_num_min = int(
                                    path_config["online_num"].split("-")[0]
                                )
                                online_num_max = int(
                                    path_config["online_num"].split("-")[1]
                                )

                                # 判断在线人数是否在此范围内
                                if (
                                    OnlineUserCount >= online_num_min
                                    and OnlineUserCount <= online_num_max
                                ):
                                    logger.debug(f"当前配置文件：{path_config['path']}")
                                    # 如果配置文件相同，则跳过
                                    if config_path == path_config["path"]:
                                        break

                                    config_path = path_config["path"]
                                    config = Config(config_path)

                                    my_handle.reload_config(config_path)

                                    logger.info(f"切换配置文件：{config_path}")

                                    break
                    except Exception as e:
                        logger.error(traceback.format_exc())

                    pass

                elif type == 8:
                    logger.info(
                        f'[分享直播间] 感谢 {data_json["User"]["Nickname"]} 分享了直播间'
                    )

                    pass

        def on_error(ws, error):
            logger.error(f"Error:{error}")

        def on_close(ws):
            logger.debug("WebSocket connection closed")

        def on_open(ws):
            logger.debug("WebSocket connection established")

        try:
            # WebSocket连接URL
            ws_url = "ws://127.0.0.1:8888"

            logger.info(f"监听地址：{ws_url}")

            # 不设置日志等级
            websocket.enableTrace(False)
            # 创建WebSocket连接
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )

            # 运行WebSocket连接
            ws.run_forever()
        except KeyboardInterrupt:
            logger.warning("程序被强行退出")
        finally:
            logger.warning(
                "关闭ws连接...请确认您是否启动了抖音弹幕监听程序，ws服务正常运行！\n监听程序启动成功后，请重新运行程序进行对接使用！"
            )
            # os._exit(0)

        # 等待子线程结束
        schedule_thread.join()
    elif platform == "dy2":
        # 源自：douyinLiveWebFetcher
        import gzip
        import string

        import requests
        import websocket

        def generateMsToken(length=107):
            """
            产生请求头部cookie中的msToken字段，其实为随机的107位字符
            :param length:字符位数
            :return:msToken
            """
            random_str = ""
            base_str = string.ascii_letters + string.digits + "=_"
            _len = len(base_str) - 1
            for _ in range(length):
                random_str += base_str[random.randint(0, _len)]
            return random_str

        def generateTtwid():
            """
            产生请求头部cookie中的ttwid字段，访问抖音网页版直播间首页可以获取到响应cookie中的ttwid
            :return: ttwid
            """
            url = "https://live.douyin.com/"
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
            except Exception as err:
                logger.info("【X】request the live url error: ", err)
            else:
                return response.cookies.get("ttwid")

        class DouyinLiveWebFetcher:
            def __init__(self, live_id):
                """
                直播间弹幕抓取对象
                :param live_id: 直播间的直播id，打开直播间web首页的链接如：https://live.douyin.com/261378947940，
                                其中的261378947940即是live_id
                """
                self.__ttwid = None
                self.__room_id = None
                self.is_connected = None
                self.live_id = live_id
                self.live_url = "https://live.douyin.com/"
                self.user_agent = (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )

            def send_heartbeat(self, ws):
                import time, threading

                def heartbeat():
                    while True:
                        time.sleep(15)  # 每15秒发送一次心跳
                        if self.is_connected:
                            ws.send("hi")  # 使用实际的心跳消息格式
                        else:
                            logger.info("Connection lost, stopping heartbeat.")
                            return

                threading.Thread(target=heartbeat).start()

            def start(self):
                self._connectWebSocket()

            def stop(self):
                self.ws.close()

            @property
            def ttwid(self):
                """
                产生请求头部cookie中的ttwid字段，访问抖音网页版直播间首页可以获取到响应cookie中的ttwid
                :return: ttwid
                """
                if self.__ttwid:
                    return self.__ttwid
                headers = {
                    "User-Agent": self.user_agent,
                }
                try:
                    response = requests.get(self.live_url, headers=headers)
                    response.raise_for_status()
                except Exception as err:
                    logger.info("【X】Request the live url error: ", err)
                else:
                    self.__ttwid = response.cookies.get("ttwid")
                    return self.__ttwid

            @property
            def room_id(self):
                """
                根据直播间的地址获取到真正的直播间roomId，有时会有错误，可以重试请求解决
                :return:room_id
                """
                if self.__room_id:
                    return self.__room_id
                url = self.live_url + self.live_id
                headers = {
                    "User-Agent": self.user_agent,
                    "cookie": f"ttwid={self.ttwid}&msToken={generateMsToken()}; __ac_nonce=0123407cc00a9e438deb4",
                }
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                except Exception as err:
                    logger.error("【X】Request the live room url error: ", err)
                    return None
                else:
                    match = re.search(r'roomId\\":\\"(\d+)\\"', response.text)
                    if match is None or len(match.groups()) < 1:
                        logger.error(
                            "【X】无法获取 真 roomId，可能是直播间号配置错了，或者被官方拉黑了"
                        )
                        return None

                    self.__room_id = match.group(1)

                    return self.__room_id

            def _connectWebSocket(self):
                """
                连接抖音直播间websocket服务器，请求直播间数据
                """
                wss = (
                    f"wss://webcast3-ws-web-lq.douyin.com/webcast/im/push/v2/?"
                    f"app_name=douyin_web&version_code=180800&webcast_sdk_version=1.3.0&update_version_code=1.3.0"
                    f"&compress=gzip"
                    f"&internal_ext=internal_src:dim|wss_push_room_id:{self.room_id}|wss_push_did:{self.room_id}"
                    f"|dim_log_id:202302171547011A160A7BAA76660E13ED|fetch_time:1676620021641|seq:1|wss_info:0-1676"
                    f"620021641-0-0|wrds_kvs:WebcastRoomStatsMessage-1676620020691146024_WebcastRoomRankMessage-167661"
                    f"9972726895075_AudienceGiftSyncData-1676619980834317696_HighlightContainerSyncData-2&cursor=t-1676"
                    f"620021641_r-1_d-1_u-1_h-1"
                    f"&host=https://live.douyin.com&aid=6383&live_id=1"
                    f"&did_rule=3&debug=false&endpoint=live_pc&support_wrds=1&"
                    f"im_path=/webcast/im/fetch/&user_unique_id={self.room_id}&"
                    f"device_platform=web&cookie_enabled=true&screen_width=1440&screen_height=900&browser_language=zh&"
                    f"browser_platform=MacIntel&browser_name=Mozilla&"
                    f"browser_version=5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20"
                    f"like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36&"
                    f"browser_online=true&tz_name=Asia/Shanghai&identity=audience&"
                    f"room_id={self.room_id}&heartbeatDuration=0&signature=00000000"
                )

                # 直接从直播间抓包ws，赋值url地址填这，在被官方拉黑的情况下用
                # wss = "wss://webcast5-ws-web-lq.douyin.com/webcast/im/push/v2/?app_name=douyin_web&version_code=180800&webcast_sdk_version=1.0.14-beta.0&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true&screen_width=2048&screen_height=1152&browser_language=zh-CN&browser_platform=Win32&browser_name=Mozilla&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36%20Edg/126.0.0.0&browser_online=true&tz_name=Etc/GMT-8&cursor=h-7383323426352862262_t-1719063974519_r-1_d-1_u-1&internal_ext=internal_src:dim|wss_push_room_id:7383264938631973686|wss_push_did:7293153952199050788|first_req_ms:1719063974385|fetch_time:1719063974519|seq:1|wss_info:0-1719063974519-0-0|wrds_v:7383323492227230262&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1&user_unique_id=7293153952199050788&im_path=/webcast/im/fetch/&identity=audience&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id=7383264938631973686&heartbeatDuration=0&signature=6DJMtCOOuubiYZP4"

                headers = {
                    "cookie": f"ttwid={self.ttwid}",
                    "user-agent": self.user_agent,
                }
                self.ws = websocket.WebSocketApp(
                    wss,
                    header=headers,
                    on_open=self._wsOnOpen,
                    on_message=self._wsOnMessage,
                    on_error=self._wsOnError,
                    on_close=self._wsOnClose,
                )
                try:
                    self.ws.run_forever()
                except Exception:
                    self.stop()
                    raise

            def _wsOnOpen(self, ws):
                """
                连接建立成功
                """
                logger.info("WebSocket connected.")
                self.is_connected = True

            def _wsOnMessage(self, ws, message):
                """
                接收到数据
                :param ws: websocket实例
                :param message: 数据
                """

                # 根据proto结构体解析对象
                package = PushFrame().parse(message)
                response = Response().parse(gzip.decompress(package.payload))

                # 返回直播间服务器链接存活确认消息，便于持续获取数据
                if response.need_ack:
                    ack = PushFrame(
                        log_id=package.log_id,
                        payload_type="ack",
                        payload=response.internal_ext.encode("utf-8"),
                    ).SerializeToString()
                    ws.send(ack, websocket.ABNF.OPCODE_BINARY)

                # 根据消息类别解析消息体
                for msg in response.messages_list:
                    method = msg.method
                    try:
                        {
                            "WebcastChatMessage": self._parseChatMsg,  # 聊天消息
                            "WebcastGiftMessage": self._parseGiftMsg,  # 礼物消息
                            "WebcastLikeMessage": self._parseLikeMsg,  # 点赞消息
                            "WebcastMemberMessage": self._parseMemberMsg,  # 进入直播间消息
                            "WebcastSocialMessage": self._parseSocialMsg,  # 关注消息
                            "WebcastRoomUserSeqMessage": self._parseRoomUserSeqMsg,  # 直播间统计
                            "WebcastFansclubMessage": self._parseFansclubMsg,  # 粉丝团消息
                            "WebcastControlMessage": self._parseControlMsg,  # 直播间状态消息
                            "WebcastEmojiChatMessage": self._parseEmojiChatMsg,  # 聊天表情包消息
                            "WebcastRoomStatsMessage": self._parseRoomStatsMsg,  # 直播间统计信息
                            "WebcastRoomMessage": self._parseRoomMsg,  # 直播间信息
                            "WebcastRoomRankMessage": self._parseRankMsg,  # 直播间排行榜信息
                        }.get(method)(msg.payload)
                    except Exception:
                        pass

            def _wsOnError(self, ws, error):
                logger.info("WebSocket error: ", error)
                self.is_connected = False

            def _wsOnClose(self, ws):
                logger.info("WebSocket connection closed.")
                self.is_connected = False

            def _parseChatMsg(self, payload):
                """聊天消息"""
                message = ChatMessage().parse(payload)
                username = message.user.nick_name
                user_id = message.user.id
                content = message.content
                logger.info(f"【聊天msg】[{user_id}]{username}: {content}")

                data = {"platform": platform, "username": username, "content": content}

                my_handle.process_data(data, "comment")

            def _parseGiftMsg(self, payload):
                """礼物消息"""
                message = GiftMessage().parse(payload)
                username = message.user.nick_name
                gift_name = message.gift.name
                num = message.combo_count
                logger.info(f"【礼物msg】{username} 送出了 {gift_name}x{num}")

                try:
                    # 暂时是写死的
                    data_path = "data/抖音礼物价格表.json"

                    # 读取JSON文件
                    with open(data_path, "r", encoding="utf-8") as file:
                        # 解析JSON数据
                        data_json = json.load(file)

                    if gift_name in data_json:
                        # 单个礼物金额 需要自己维护礼物价值表
                        discount_price = data_json[gift_name]
                    else:
                        logger.warning(
                            f"数据文件：{data_path} 中，没有 {gift_name} 对应的价值，请手动补充数据"
                        )
                        discount_price = 1
                except Exception as e:
                    logger.error(traceback.format_exc())
                    discount_price = 1

                # 总金额
                combo_total_coin = num * discount_price

                data = {
                    "platform": platform,
                    "gift_name": gift_name,
                    "username": username,
                    "num": num,
                    "unit_price": discount_price / 10,
                    "total_price": combo_total_coin / 10,
                }

                my_handle.process_data(data, "gift")

            def _parseLikeMsg(self, payload):
                """点赞消息"""
                message = LikeMessage().parse(payload)
                user_name = message.user.nick_name
                count = message.count
                logger.info(f"【点赞msg】{user_name} 点了{count}个赞")

            def _parseMemberMsg(self, payload):
                """进入直播间消息"""
                message = MemberMessage().parse(payload)
                username = message.user.nick_name
                user_id = message.user.id
                gender = ["女", "男"][message.user.gender]
                logger.info(f"【进场msg】[{user_id}][{gender}]{username} 进入了直播间")

                data = {
                    "platform": platform,
                    "username": username,
                    "content": "进入直播间",
                }

                # 添加用户名到最新的用户名列表
                add_username_to_last_username_list(username)

                my_handle.process_data(data, "entrance")

            def _parseSocialMsg(self, payload):
                """关注消息"""
                message = SocialMessage().parse(payload)
                user_name = message.user.nick_name
                user_id = message.user.id
                logger.info(f"【关注msg】[{user_id}]{user_name} 关注了主播")

                data = {"platform": platform, "username": username}

                my_handle.process_data(data, "follow")

            def _parseRoomUserSeqMsg(self, payload):
                """直播间统计"""
                message = RoomUserSeqMessage().parse(payload)
                OnlineUserCount = message.total
                total = message.total_pv_for_anchor
                logger.info(
                    f"【统计msg】当前观看人数: {OnlineUserCount}, 累计观看人数: {total}"
                )

                try:
                    global last_liveroom_data

                    # {'OnlineUserCount': 50, 'TotalUserCount': 22003, 'TotalUserCountStr': '2.2万', 'OnlineUserCountStr': '50',
                    # 'MsgId': 7260517442466662207, 'User': None, 'Content': '当前直播间人数 50，累计直播间人数 2.2万', 'RoomId': 7260415920948906807}
                    # logger.info(f"data_json={data_json}")

                    last_liveroom_data = {
                        "OnlineUserCount": OnlineUserCount,
                        "TotalUserCountStr": total,
                    }

                    # 是否开启了动态配置功能
                    if config.get("trends_config", "enable"):
                        for path_config in config.get("trends_config", "path"):
                            online_num_min = int(
                                path_config["online_num"].split("-")[0]
                            )
                            online_num_max = int(
                                path_config["online_num"].split("-")[1]
                            )

                            # 判断在线人数是否在此范围内
                            if (
                                OnlineUserCount >= online_num_min
                                and OnlineUserCount <= online_num_max
                            ):
                                logger.debug(f"当前配置文件：{path_config['path']}")
                                # 如果配置文件相同，则跳过
                                if config_path == path_config["path"]:
                                    break

                                config_path = path_config["path"]
                                config = Config(config_path)

                                my_handle.reload_config(config_path)

                                logger.info(f"切换配置文件：{config_path}")

                                break
                except Exception as e:
                    logger.error(traceback.format_exc())

                pass

            def _parseFansclubMsg(self, payload):
                """粉丝团消息"""
                message = FansclubMessage().parse(payload)
                content = message.content
                logger.info(f"【粉丝团msg】 {content}")

            def _parseEmojiChatMsg(self, payload):
                """聊天表情包消息"""
                message = EmojiChatMessage().parse(payload)
                emoji_id = message.emoji_id
                user = message.user
                common = message.common
                default_content = message.default_content
                logger.info(
                    f"【聊天表情包id】 {emoji_id},user：{user},common:{common},default_content:{default_content}"
                )

            def _parseRoomMsg(self, payload):
                message = RoomMessage().parse(payload)
                common = message.common
                room_id = common.room_id
                logger.info(f"【直播间msg】直播间id:{room_id}")

            def _parseRoomStatsMsg(self, payload):
                message = RoomStatsMessage().parse(payload)
                display_long = message.display_long
                logger.info(f"【直播间统计msg】{display_long}")

            def _parseRankMsg(self, payload):
                message = RoomRankMessage().parse(payload)
                ranks_list = message.ranks_list
                logger.info(f"【直播间排行榜msg】{ranks_list}")

            def _parseControlMsg(self, payload):
                """直播间状态消息"""
                message = ControlMessage().parse(payload)

                if message.status == 3:
                    logger.info("直播间已结束")
                    self.stop()

        config_room_id = my_handle.get_room_id()
        DouyinLiveWebFetcher(config_room_id).start()

    elif platform == "ks2":
        import websockets

        async def on_message(websocket, path):
            global last_liveroom_data, last_username_list
            global global_idle_time

            async for message in websocket:
                # logger.info(f"收到消息: {message}")
                # await websocket.send("服务器收到了你的消息: " + message)

                try:
                    data_json = json.loads(message)
                    # logger.debug(data_json)
                    if data_json["type"] == "comment":
                        # logger.info(data_json)
                        # 闲时计数清零
                        idle_time_auto_clear("comment")

                        username = data_json["username"]
                        content = data_json["content"]

                        logger.info(f"[📧直播间弹幕消息] [{username}]：{content}")

                        data = {
                            "platform": platform,
                            "username": username,
                            "content": content,
                        }

                        my_handle.process_data(data, "comment")

                        # 添加用户名到最新的用户名列表
                        add_username_to_last_username_list(username)

                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error("数据解析错误！")
                    my_handle.abnormal_alarm_handle("platform")
                    continue

        async def ws_server():
            ws_url = "127.0.0.1"
            ws_port = 5000
            server = await websockets.serve(on_message, ws_url, ws_port)
            logger.info(f"WebSocket 服务器已在 {ws_url}:{ws_port} 启动")
            await server.wait_closed()

        asyncio.run(ws_server())

    elif platform == "ks":
        from playwright.sync_api import sync_playwright, TimeoutError
        from google.protobuf.json_format import MessageToDict
        from configparser import ConfigParser
        import kuaishou_pb2

        class kslive(object):
            def __init__(self):
                global config, common, my_handle

                self.path = os.path.abspath("")
                self.chrome_path = r"\firefox-1419\firefox\firefox.exe"
                self.ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
                self.uri = "https://live.kuaishou.com/u/"
                self.context = None
                self.browser = None
                self.page = None

                try:
                    self.live_ids = config.get("room_display_id")
                    self.thread = 2
                    # 没什么用的手机号配置，也就方便登录
                    self.phone = "123"
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error("请检查配置文件")
                    my_handle.abnormal_alarm_handle("platform")
                    exit()

            def find_file(self, find_path, file_type) -> list:
                """
                寻找文件
                :param find_path: 子路径
                :param file_type: 文件类型
                :return:
                """
                path = self.path + "\\" + find_path
                data_list = []
                for root, dirs, files in os.walk(path):
                    if root != path:
                        break
                    for file in files:
                        file_path = os.path.join(root, file)
                        if file_path.find(file_type) != -1:
                            data_list.append(file_path)
                return data_list

            def main(self, lid, semaphore):
                if not os.path.exists(self.path + "\\cookie"):
                    os.makedirs(self.path + "\\cookie")

                cookie_path = self.path + "\\cookie\\" + self.phone + ".json"
                # if not os.path.exists(cookie_path):
                #     with open(cookie_path, 'w') as file:
                #         file.write('{"a":"a"}')
                #     logger.info(f"'{cookie_path}' 创建成功")
                # else:
                #     logger.info(f"'{cookie_path}' 已存在，无需创建")

                with semaphore:
                    thread_name = threading.current_thread().name.split("-")[0]
                    with sync_playwright() as p:
                        self.browser = p.chromium.launch(headless=False)
                        # self.browser = p.firefox.launch(headless=False)
                        # executable_path=self.path + self.chrome_path
                        cookie_list = self.find_file("cookie", "json")

                        live_url = self.uri + lid

                        if not os.path.exists(cookie_path):
                            self.context = self.browser.new_context(
                                storage_state=None, user_agent=self.ua
                            )
                        else:
                            self.context = self.browser.new_context(
                                storage_state=cookie_list[0], user_agent=self.ua
                            )
                        self.page = self.context.new_page()
                        self.page.add_init_script(
                            "Object.defineProperties(navigator, {webdriver:{get:()=>undefined}});"
                        )
                        self.page.goto("https://live.kuaishou.com/")
                        # self.page.goto(live_url)
                        element = self.page.get_attribute(".no-login", "style")

                        if not element:
                            logger.info("未登录，请先登录~")
                            self.page.locator(".login").click()
                            self.page.locator(
                                "li.tab-panel:nth-child(2) > h4:nth-child(1)"
                            ).click()
                            self.page.locator(
                                "div.normal-login-item:nth-child(1) > div:nth-child(1) > input:nth-child(1)"
                            ).fill(self.phone)
                        try:
                            self.page.wait_for_selector(
                                "#app > section > div.header-placeholder > header > div.header-main > "
                                "div.right-part > div.user-info > div.tooltip-trigger > span",
                                timeout=1000 * 60 * 2,
                            )
                            if not os.path.exists(self.path + "\\cookie"):
                                os.makedirs(self.path + "\\cookie")
                            self.context.storage_state(path=cookie_path)
                            # 检测是否开播
                            selector = (
                                "html body div#app div.live-room div.detail div.player "
                                "div.kwai-player.kwai-player-container.kwai-player-rotation-0 "
                                "div.kwai-player-container-video div.kwai-player-plugins div.center-state div.state "
                                "div.no-live-detail div.desc p.tip"
                            )  # 检测正在直播时下播的选择器
                            try:
                                msg = self.page.locator(selector).text_content(
                                    timeout=3000
                                )
                                logger.info("当前%s" % thread_name + "，" + msg)
                                self.context.close()
                                self.browser.close()

                            except Exception as e:
                                logger.info("当前%s，[%s]正在直播" % (thread_name, lid))

                                logger.info(f"跳转直播间：{live_url}")
                                # self.page.goto(live_url)
                                # time.sleep(1)

                                self.page.goto(live_url)

                                # 等待一段时间检查是否有验证码弹窗
                                try:
                                    captcha_selector = "html body div.container"  # 假设这是验证码弹窗的选择器
                                    self.page.wait_for_selector(
                                        captcha_selector, timeout=5000
                                    )  # 等待5秒看是否出现验证码
                                    logger.info("检测到验证码，处理验证码...")
                                    # 等待验证码弹窗从DOM中被完全移除
                                    self.page.wait_for_selector(
                                        captcha_selector,
                                        state="detached",
                                        timeout=10000,
                                    )  # 假设最长等待10秒验证码验证完成
                                    logger.info("验证码已验证，弹窗已移除")
                                    # 弹窗处理逻辑之后等待1秒
                                    time.sleep(1)
                                    # 处理完验证码后，可能需要再次跳转页面
                                    # self.page.goto(live_url)
                                except TimeoutError:
                                    logger.error("没有检测到验证码，继续执行...")

                                logger.info(f"请在10s内手动打开直播间：{live_url}")

                                time.sleep(10)

                                self.page.on("websocket", self.web_sockets)
                                logger.info(f"24h监听直播间等待下播...")
                                self.page.wait_for_selector(selector, timeout=86400000)
                                logger.error(
                                    "当前%s，[%s]的直播结束了" % (thread_name, lid)
                                )
                                self.context.close()
                                self.browser.close()

                        except Exception as e:
                            logger.error(traceback.format_exc())
                            self.context.close()
                            self.browser.close()

            def web_sockets(self, web_socket):
                logger.info("web_sockets...")
                urls = web_socket.url
                logger.info(urls)
                if "/websocket" in urls:
                    logger.info("websocket连接成功，创建监听事件")
                    web_socket.on("close", self.websocket_close)
                    web_socket.on("framereceived", self.handler)

            def websocket_close(self):
                self.context.close()
                self.browser.close()

            def handler(self, websocket):
                Message = kuaishou_pb2.SocketMessage()
                Message.ParseFromString(websocket)
                if Message.payloadType == 310:
                    SCWebFeedPUsh = kuaishou_pb2.SCWebFeedPush()
                    SCWebFeedPUsh.ParseFromString(Message.payload)
                    obj = MessageToDict(SCWebFeedPUsh, preserving_proto_field_name=True)

                    logger.debug(obj)

                    if obj.get("commentFeeds", ""):
                        msg_list = obj.get("commentFeeds", "")
                        for i in msg_list:
                            # 闲时计数清零
                            idle_time_auto_clear("comment")

                            username = i["user"]["userName"]
                            pid = i["user"]["principalId"]
                            content = i["content"]
                            logger.info(f"[📧直播间弹幕消息] [{username}]:{content}")

                            data = {
                                "platform": platform,
                                "username": username,
                                "content": content,
                            }

                            my_handle.process_data(data, "comment")
                    if obj.get("giftFeeds", ""):
                        idle_time_auto_clear("gift")

                        msg_list = obj.get("giftFeeds", "")
                        for i in msg_list:
                            username = i["user"]["userName"]
                            # pid = i['user']['principalId']
                            giftId = i["giftId"]
                            comboCount = i["comboCount"]
                            logger.info(
                                f"[🎁直播间礼物消息] 用户：{username} 赠送礼物Id={giftId} 连击数={comboCount}"
                            )
                    if obj.get("likeFeeds", ""):
                        msg_list = obj.get("likeFeeds", "")
                        for i in msg_list:
                            username = i["user"]["userName"]
                            pid = i["user"]["principalId"]
                            logger.info(f"{username}")

        class run(kslive):
            def __init__(self):
                super().__init__()
                self.ids_list = self.live_ids.split(",")

            def run_live(self):
                """
                主程序入口
                :return:
                """
                t_list = []
                # 允许的最大线程数
                if self.thread < 1:
                    self.thread = 1
                elif self.thread > 8:
                    self.thread = 8
                    logger.info("线程最大允许8，线程数最好设置cpu核心数")

                semaphore = threading.Semaphore(self.thread)
                # 用于记录数量
                n = 0
                if not self.live_ids:
                    logger.info("请导入网页直播id，多个以','间隔")
                    return

                for i in self.ids_list:
                    n += 1
                    t = threading.Thread(
                        target=kslive().main, args=(i, semaphore), name=f"线程：{n}-{i}"
                    )
                    t.start()
                    t_list.append(t)
                for i in t_list:
                    i.join()

        run().run_live()
    elif platform in ["pdd", "douyu", "1688", "taobao"]:
        import websockets

        async def on_message(websocket, path):
            global last_liveroom_data, last_username_list
            global global_idle_time

            async for message in websocket:
                # logger.info(f"收到消息: {message}")
                # await websocket.send("服务器收到了你的消息: " + message)

                try:
                    data_json = json.loads(message)
                    # logger.debug(data_json)
                    if data_json["type"] == "comment":
                        # logger.info(data_json)
                        # 闲时计数清零
                        idle_time_auto_clear("comment")

                        username = data_json["username"]
                        content = data_json["content"]

                        logger.info(f"[📧直播间弹幕消息] [{username}]：{content}")

                        data = {
                            "platform": platform,
                            "username": username,
                            "content": content,
                        }

                        my_handle.process_data(data, "comment")

                        # 添加用户名到最新的用户名列表
                        add_username_to_last_username_list(username)

                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error("数据解析错误！")
                    my_handle.abnormal_alarm_handle("platform")
                    continue

        async def ws_server():
            ws_url = "127.0.0.1"
            ws_port = 5000
            server = await websockets.serve(on_message, ws_url, ws_port)
            logger.info(f"WebSocket 服务器已在 {ws_url}:{ws_port} 启动")
            await server.wait_closed()

        asyncio.run(ws_server())
    elif platform == "tiktok":
        """
        tiktok
        """
        from TikTokLive import TikTokLiveClient
        from TikTokLive.events import (
            CommentEvent,
            ConnectEvent,
            DisconnectEvent,
            JoinEvent,
            GiftEvent,
            FollowEvent,
        )
        # from TikTokLive.client.errors import LiveNotFound

        # 比如直播间是 https://www.tiktok.com/@username/live 那么room_id就是 username，其实就是用户唯一ID
        room_id = my_handle.get_room_id()

        proxys = {
            "http://": "http://127.0.0.1:10809",
            "https://": "http://127.0.0.1:10809",
        }

        proxys = None

        # 代理软件开启TUN模式进行代理，由于库的ws不走传入的代理参数，只能靠代理软件全代理了
        client: TikTokLiveClient = TikTokLiveClient(
            unique_id=f"@{room_id}", web_proxy=proxys, ws_proxy=proxys
        )

        def start_client():
            # Define how you want to handle specific events via decorator
            @client.on("connect")
            async def on_connect(_: ConnectEvent):
                logger.info(f"连接到 房间ID:{client.room_id}")

            @client.on("disconnect")
            async def on_disconnect(event: DisconnectEvent):
                logger.info("断开连接，10秒后重连")
                await asyncio.sleep(10)  # 等待一段时间后尝试重连，这里等待10秒
                start_client()  # 尝试重新连接

            @client.on("join")
            async def on_join(event: JoinEvent):
                idle_time_auto_clear("entrance")

                username = event.user.nickname
                unique_id = event.user.unique_id

                logger.info(f"[🚹🚺直播间成员加入消息] 欢迎 {username} 进入直播间")

                data = {
                    "platform": platform,
                    "username": username,
                    "content": "进入直播间",
                }

                # 添加用户名到最新的用户名列表
                add_username_to_last_username_list(username)

                my_handle.process_data(data, "entrance")

            # Notice no decorator?
            @client.on("comment")
            async def on_comment(event: CommentEvent):
                # 闲时计数清零
                idle_time_auto_clear("comment")

                username = event.user.nickname
                content = event.comment

                logger.info(f"[📧直播间弹幕消息] [{username}]：{content}")

                data = {"platform": platform, "username": username, "content": content}

                my_handle.process_data(data, "comment")

            @client.on("gift")
            async def on_gift(event: GiftEvent):
                """
                This is an example for the "gift" event to show you how to read gift data properly.

                Important Note:

                Gifts of type 1 can have streaks, so we need to check that the streak has ended
                If the gift type isn't 1, it can't repeat. Therefore, we can go straight to logger.infoing

                """
                idle_time_auto_clear("gift")

                # Streakable gift & streak is over
                if event.gift.streakable and not event.gift.streaking:
                    # 礼物重复数量
                    repeat_count = event.gift.count

                # Non-streakable gift
                elif not event.gift.streakable:
                    # 礼物重复数量
                    repeat_count = 1

                gift_name = event.gift.info.name
                username = event.user.nickname
                # 礼物数量
                num = 1

                try:
                    # 暂时是写死的
                    data_path = "data/tiktok礼物价格表.json"

                    # 读取JSON文件
                    with open(data_path, "r", encoding="utf-8") as file:
                        # 解析JSON数据
                        data_json = json.load(file)

                    if gift_name in data_json:
                        # 单个礼物金额 需要自己维护礼物价值表
                        discount_price = data_json[gift_name]
                    else:
                        logger.warning(
                            f"数据文件：{data_path} 中，没有 {gift_name} 对应的价值，请手动补充数据"
                        )
                        discount_price = 1
                except Exception as e:
                    logger.error(traceback.format_exc())
                    discount_price = 1

                # 总金额
                combo_total_coin = repeat_count * discount_price

                logger.info(
                    f"[🎁直播间礼物消息] 用户：{username} 赠送 {num} 个 {gift_name}，单价 {discount_price}抖币，总计 {combo_total_coin}抖币"
                )

                data = {
                    "platform": platform,
                    "gift_name": gift_name,
                    "username": username,
                    "num": num,
                    "unit_price": discount_price / 10,
                    "total_price": combo_total_coin / 10,
                }

                my_handle.process_data(data, "gift")

            @client.on("follow")
            async def on_follow(event: FollowEvent):
                idle_time_auto_clear("follow")

                username = event.user.nickname

                logger.info(f"[➕直播间关注消息] 感谢 {username} 的关注")

                data = {"platform": platform, "username": username}

                my_handle.process_data(data, "follow")

            try:
                client.stop()
                logger.info(f"连接{room_id}中...")
                client.run()

            except Exception as e:
                logger.info(f"用户ID: @{client.unique_id} 好像不在线捏, 1分钟后重试...")
                start_client()

        # 运行客户端
        start_client()
    elif platform == "twitch":
        import socks
        from emoji import demojize

        try:
            server = "irc.chat.twitch.tv"
            port = 6667
            nickname = "主人"

            try:
                channel = (
                    "#" + config.get("room_display_id")
                )  # 要从中检索消息的频道，注意#必须携带在头部 The channel you want to retrieve messages from
                token = config.get(
                    "twitch", "token"
                )  # 访问 https://twitchapps.com/tmi/ 获取
                user = config.get(
                    "twitch", "user"
                )  # 你的Twitch用户名 Your Twitch username
                # 代理服务器的地址和端口
                proxy_server = config.get("twitch", "proxy_server")
                proxy_port = int(config.get("twitch", "proxy_port"))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("获取Twitch配置失败！\n{0}".format(e))
                my_handle.abnormal_alarm_handle("platform")

            # 配置代理服务器
            socks.set_default_proxy(socks.HTTP, proxy_server, proxy_port)

            # 创建socket对象
            sock = socks.socksocket()

            try:
                sock.connect((server, port))
                logger.info("成功连接 Twitch IRC server")
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(f"连接 Twitch IRC server 失败: {e}")
                my_handle.abnormal_alarm_handle("platform")

            sock.send(f"PASS {token}\n".encode("utf-8"))
            sock.send(f"NICK {nickname}\n".encode("utf-8"))
            sock.send(f"JOIN {channel}\n".encode("utf-8"))

            regex = r":(\w+)!\w+@\w+\.tmi\.twitch\.tv PRIVMSG #\w+ :(.+)"

            # 重连次数
            retry_count = 0

            while True:
                try:
                    resp = sock.recv(2048).decode("utf-8")

                    # 输出所有接收到的内容，包括PING/PONG
                    # logger.info(resp)

                    if resp.startswith("PING"):
                        sock.send("PONG\n".encode("utf-8"))

                    elif not user in resp:
                        # 闲时计数清零
                        idle_time_auto_clear("comment")

                        resp = demojize(resp)

                        logger.debug(resp)

                        match = re.match(regex, resp)

                        username = match.group(1)
                        content = match.group(2)
                        content = content.rstrip()

                        logger.info(f"[{username}]: {content}")

                        data = {
                            "platform": platform,
                            "username": username,
                            "content": content,
                        }

                        my_handle.process_data(data, "comment")
                except AttributeError as e:
                    logger.error(traceback.format_exc())
                    logger.error(f"捕获到异常: {e}")
                    logger.error("发生异常，重新连接socket")
                    my_handle.abnormal_alarm_handle("platform")

                    if retry_count >= 3:
                        logger.error(f"多次重连失败，程序结束！")
                        return

                    retry_count += 1
                    logger.error(f"重试次数: {retry_count}")

                    # 在这里添加重新连接socket的代码
                    # 例如，你可能想要关闭旧的socket连接，然后重新创建一个新的socket连接
                    sock.close()

                    # 创建socket对象
                    sock = socks.socksocket()

                    try:
                        sock.connect((server, port))
                        logger.info("成功连接 Twitch IRC server")
                    except Exception as e:
                        logger.error(f"连接 Twitch IRC server 失败: {e}")

                    sock.send(f"PASS {token}\n".encode("utf-8"))
                    sock.send(f"NICK {nickname}\n".encode("utf-8"))
                    sock.send(f"JOIN {channel}\n".encode("utf-8"))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error("Error receiving chat: {0}".format(e))
                    my_handle.abnormal_alarm_handle("platform")
        except Exception as e:
            logger.error(traceback.format_exc())
            my_handle.abnormal_alarm_handle("platform")
    elif platform == "wxlive":
        import uvicorn
        from fastapi import FastAPI, Request
        from fastapi.middleware.cors import CORSMiddleware
        from utils.models import SendMessage, LLMMessage, CallbackMessage, CommonResult

        # 定义FastAPI应用
        app = FastAPI()
        seq_list = []

        # 允许跨域
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @app.post("/wxlive")
        async def wxlive(request: Request):
            global my_handle, config

            try:
                # 获取 POST 请求中的数据
                data = await request.json()
                # 这里可以添加代码处理接收到的数据
                logger.debug(data)

                if data["events"][0]["seq"] in seq_list:
                    return CommonResult(code=-1, message="重复数据过滤")

                # 如果列表长度达到30，移除最旧的元素
                if len(seq_list) >= 30:
                    seq_list.pop(0)

                # 添加新元素
                seq_list.append(data["events"][0]["seq"])

                # 弹幕数据
                if data["events"][0]["decoded_type"] == "comment":
                    # 闲时计数清零
                    idle_time_auto_clear("comment")

                    content = data["events"][0]["content"]  # 获取弹幕内容
                    username = data["events"][0]["nickname"]  # 获取发送弹幕的用户昵称

                    logger.info(f"[{username}]: {content}")

                    data = {
                        "platform": platform,
                        "username": username,
                        "content": content,
                    }

                    my_handle.process_data(data, "comment")
                # 入场数据
                elif data["events"][0]["decoded_type"] == "enter":
                    idle_time_auto_clear("entrance")

                    username = data["events"][0]["nickname"]

                    logger.info(f"用户：{username} 进入直播间")

                    # 添加用户名到最新的用户名列表
                    add_username_to_last_username_list(username)

                    data = {
                        "platform": platform,
                        "username": username,
                        "content": "进入直播间",
                    }

                    my_handle.process_data(data, "entrance")
                    pass

                # 响应
                return CommonResult(code=200, message="成功接收")
            except Exception as e:
                logger.error(traceback.format_exc())
                my_handle.abnormal_alarm_handle("platform")
                return CommonResult(code=-1, message=f"发送数据失败！{e}")

        # 定义POST请求路径和处理函数
        @app.post("/send")
        async def send(msg: SendMessage):
            global my_handle, config

            try:
                tmp_json = msg.dict()
                logger.info(f"API收到数据：{tmp_json}")
                data_json = tmp_json["data"]
                if "type" not in data_json:
                    data_json["type"] = tmp_json["type"]

                if data_json["type"] in ["reread", "reread_top_priority"]:
                    my_handle.reread_handle(data_json, type=data_json["type"])
                elif data_json["type"] == "comment":
                    my_handle.process_data(data_json, "comment")
                elif data_json["type"] == "tuning":
                    my_handle.tuning_handle(data_json)
                elif data_json["type"] == "gift":
                    my_handle.gift_handle(data_json)
                elif data_json["type"] == "entrance":
                    my_handle.entrance_handle(data_json)

                return CommonResult(code=200, message="成功")
            except Exception as e:
                logger.error(f"发送数据失败！{e}")
                return CommonResult(code=-1, message=f"发送数据失败！{e}")

        @app.post("/llm")
        async def llm(msg: LLMMessage):
            global my_handle, config

            try:
                data_json = msg.dict()
                logger.info(f"API收到数据：{data_json}")

                resp_content = my_handle.llm_handle(
                    data_json["type"], data_json, webui_show=False
                )

                return CommonResult(
                    code=200, message="成功", data={"content": resp_content}
                )
            except Exception as e:
                logger.error(f"调用LLM失败！{e}")
                return CommonResult(code=-1, message=f"调用LLM失败！{e}")

        @app.post("/callback")
        async def callback(msg: CallbackMessage):
            global my_handle, config, global_idle_time

            try:
                data_json = msg.dict()
                logger.info(f"API收到数据：{data_json}")

                # 音频播放完成
                if data_json["type"] in ["audio_playback_completed"]:
                    # 如果等待播放的音频数量大于10
                    if data_json["data"]["wait_play_audio_num"] > int(
                        config.get("idle_time_task", "wait_play_audio_num_threshold")
                    ):
                        logger.info(
                            f'等待播放的音频数量大于限定值，闲时任务的闲时计时由 {global_idle_time} -> {int(config.get("idle_time_task", "idle_time_reduce_to"))}秒'
                        )
                        # 闲时任务的闲时计时 清零
                        global_idle_time = int(
                            config.get("idle_time_task", "idle_time_reduce_to")
                        )

                return CommonResult(code=200, message="callback处理成功！")
            except Exception as e:
                logger.error(f"callback处理失败！{e}")
                return CommonResult(code=-1, message=f"callback处理失败！{e}")

        logger.info("HTTP API线程已启动！")
        uvicorn.run(app, host="0.0.0.0", port=config.get("api_port"))

    elif platform == "youtube":
        import pytchat

        def get_video_id():
            try:
                return config.get("room_display_id")
            except Exception as e:
                logger.error("获取直播间号失败！\n{0}".format(e))
                return None

        def process_chat(live):
            while live.is_alive():
                try:
                    for c in live.get().sync_items():
                        # 过滤表情包
                        chat_raw = re.sub(r":[^\s]+:", "", c.message)
                        chat_raw = chat_raw.replace("#", "")
                        if chat_raw != "":
                            # 闲时计数清零
                            idle_time_auto_clear("comment")

                            content = chat_raw  # 获取弹幕内容
                            username = c.author.name  # 获取发送弹幕的用户昵称

                            logger.info(f"[{username}]: {content}")

                            data = {
                                "platform": platform,
                                "username": username,
                                "content": content,
                            }

                            my_handle.process_data(data, "comment")

                        # time.sleep(1)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error("Error receiving chat: {0}".format(e))
                    my_handle.abnormal_alarm_handle("platform")
                    break  # 退出内部while循环以触发重连机制

        try:
            reconnect_attempts = 0
            last_reconnect_time = None

            while True:
                video_id = get_video_id()
                if video_id is None:
                    break

                live = pytchat.create(video_id=video_id)
                process_chat(live)

                current_time = time.time()
                # 如果重连间隔只有30s内，那就只有3次，如果间隔大于30s，那就无限重连
                if last_reconnect_time and (current_time - last_reconnect_time < 30):
                    reconnect_attempts += 1
                    if reconnect_attempts >= 3:
                        logger.error("重连失败次数已达上限，退出程序...")
                        break
                    logger.warning(
                        f"连接已关闭，间隔小于30秒，尝试重新连接 ({reconnect_attempts}/3)..."
                    )
                else:
                    reconnect_attempts = 0  # 重置重连次数
                    logger.warning("连接已关闭，尝试重新连接...")

                last_reconnect_time = current_time

        except KeyboardInterrupt:
            logger.warning("程序被强行退出")

        finally:
            logger.warning("关闭连接...")
            os._exit(0)
    elif platform == "hntv":
        import requests

        # 初始化已获取的commentId集合
        comment_set = set()

        def fetch_comments():
            try:
                url = f"https://pubmod.hntv.tv/dx-bridge/get-comment-with-article-super-v2?limit=40&typeId=1&appFusionId=1390195608019869697&page=1&objectId={my_handle.get_room_id()}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("result", {}).get("items", [])
                    for item in items:
                        comment_id = item.get("commentId")
                        if comment_id not in comment_set:
                            comment_set.add(comment_id)
                            username = item.get("commentUserNickname", "")
                            content = item.get("content", "")

                            logger.info(f"[{username}]: {content}")

                            data = {
                                "platform": platform,
                                "username": username,
                                "content": content,
                            }

                            my_handle.process_data(data, "comment")
                else:
                    logger.error("获取弹幕数据失败。。。")
            except Exception as e:
                logger.error(traceback.format_exc())
                my_handle.abnormal_alarm_handle("platform")

        while True:
            fetch_comments()
            time.sleep(3)  # 每隔3秒轮询一次
    elif platform == "ordinaryroad_barrage_fly":
        from asyncio import Event
        from contextlib import asynccontextmanager
        from datetime import timedelta
        from typing import AsyncGenerator, Tuple

        from reactivestreams.subscriber import Subscriber
        from reactivestreams.subscription import Subscription
        from rsocket.helpers import single_transport_provider
        from rsocket.payload import Payload
        from rsocket.rsocket_client import RSocketClient
        from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
        from rsocket.transports.aiohttp_websocket import TransportAioHttpClient

        subscribe_payload_json = {
            "data": {
                "taskIds": [],
                "cmd": "SUBSCRIBE"
            }
        }


        class ChannelSubscriber(Subscriber):
            def __init__(self, wait_for_responder_complete: Event) -> None:
                super().__init__()
                self.subscription = None
                self._wait_for_responder_complete = wait_for_responder_complete

            def on_subscribe(self, subscription: Subscription):
                self.subscription = subscription
                self.subscription.request(0x7FFFFFFF)

            # TODO 收到消息回调
            def on_next(self, value: Payload, is_complete=False):
                try:
                    msg_dto = json.loads(value.data)
                    if type(msg_dto) != dict:
                        return
                    msg_type = msg_dto.get('type')
                    # 直接输出
                    if msg_type == "DANMU":
                        msg = msg_dto['msg']
                        # logger.info(
                        #     f"{msg_dto['roomId']} 收到弹幕 {str(msg['badgeLevel']) + str(msg['badgeName']) if msg['badgeLevel'] != 0 else ''} {msg['username']}({str(msg['uid'])})：{msg['content']}"
                        # )
                        username = msg['username']
                        content = msg['content']
                        logger.info(f"【让弹幕飞-{msg_dto['platform']}-{msg_dto['roomId']}】 [{username}]: {content}")

                        data = {
                            "platform": platform,
                            "username": username,
                            "content": content,
                        }

                        my_handle.process_data(data, "comment")
                    elif msg_type == "GIFT":
                        msg = msg_dto['msg']
                        logger.debug(msg)
                        # logger.info(
                        #     f"{msg_dto['roomId']} 收到礼物 {str(msg['badgeLevel']) + str(msg['badgeName']) if msg['badgeLevel'] != 0 else ''} {msg['username']}({str(msg['uid'])}) {str(msg['data']['action']) if msg.get('data') is not None and msg.get('data').get('action') is not None else '赠送'} {msg['giftName']}({str(msg['giftId'])})x{str(msg['giftCount'])}({str(msg['giftPrice'])})"
                        # )
                        username = msg['username']
                        gift_name = msg['giftName']
                        combo_num = msg['giftCount']
                        combo_total_coin = combo_num * msg['giftPrice']
                        logger.info(
                            f"【让弹幕飞-{msg_dto['platform']}-{msg_dto['roomId']}】 [{username}] 赠送 {combo_num} 个 {gift_name}，总计 {combo_total_coin}"
                        )

                        # TODO： 金额换算
                        data = {
                            "platform": platform,
                            "gift_name": gift_name,
                            "username": username,
                            # "user_face": user_face,
                            "num": combo_num,
                            "unit_price": combo_total_coin / combo_num,
                            "total_price": combo_total_coin,
                        }

                        my_handle.process_data(data, "gift")
                    elif msg_type == "ENTER_ROOM":
                        msg = msg_dto['msg']
                        username = msg['username']
                        logger.info(f"【让弹幕飞-{msg_dto['platform']}-{msg_dto['roomId']}】 欢迎 {username} 进入直播间")

                        data = {
                            "platform": platform,
                            "username": username,
                            "content": "进入直播间",
                        }

                        # 添加用户名到最新的用户名列表
                        add_username_to_last_username_list(username)

                        my_handle.process_data(data, "entrance")
                    elif msg_type == "LIKE":
                        msg = msg_dto['msg']
                        logger.debug(msg)
                        username = msg['username']
                        clickCount = msg['clickCount']
                        logger.info(f"【让弹幕飞-{msg_dto['platform']}-{msg_dto['roomId']}】 [{username}] 点赞了 {clickCount} 次")
                    # 无用消息丢弃
                    elif msg_type in ["inter_h5_game_data_update"]:
                        pass
                    else:
                        # 刚连接上ws收到的消息
                        if "status" in msg_dto:
                            pass
                        else:
                            logger.debug(msg_dto)
                            logger.debug(f"【让弹幕飞-{msg_dto['platform']}-{msg_dto['roomId']}】 收到消息 " + json.dumps(msg_dto))
                    if is_complete:
                        self._wait_for_responder_complete.set()
                except Exception as e:
                    logger.error(traceback.format_exc())

            def on_error(self, exception: Exception):
                logger.error('Error from server on channel' + str(exception))
                self._wait_for_responder_complete.set()

            def on_complete(self):
                logger.info('Completed from server on channel')
                self._wait_for_responder_complete.set()


        @asynccontextmanager
        async def connect(websocket_uri):
            """
            创建一个Client，建立连接并return
            """
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(websocket_uri) as websocket:
                        async with RSocketClient(
                                single_transport_provider(TransportAioHttpClient(websocket=websocket)),
                                keep_alive_period=timedelta(seconds=30),
                                max_lifetime_period=timedelta(days=1)
                        ) as client:
                            yield client
            except Exception as e:
                logger.error(traceback.format_exc())

        async def main(websocket_uri):
            try:
                # 1 建立连接
                async with connect(websocket_uri) as client:
                    # 阻塞等待Channel关闭事件
                    channel_completion_event = Event()

                    # 定义Client向Channel发送消息的Publisher
                    # Python没有匿名内部类，这里定义一个方法作为参数，传给StreamFromAsyncGenerator类
                    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
                        # 2 发送订阅Task的请求
                        # Payload：Client通过Channel向Server发送的消息，False表示不需要关闭Channel
                        yield Payload(
                            data=json.dumps(subscribe_payload_json["data"]).encode()
                        ), False
                        # 发送了一条订阅消息后直接暂停发送即可
                        await Event().wait()

                    stream = StreamFromAsyncGenerator(generator)

                    # Client请求一个Channel，Payload留空，turn StreamHandler
                    requested = client.request_channel(Payload(), stream)

                    # 3 订阅Channel，ChannelSubscriber用于处理Server通过Channel回复的消息
                    requested.subscribe(ChannelSubscriber(channel_completion_event))

                    await channel_completion_event.wait()
            except Exception as e:
                logger.error(traceback.format_exc())
                my_handle.abnormal_alarm_handle("platform")

        if config.get("ordinaryroad_barrage_fly", "taskIds") == []:
            logger.error("请先配置 让弹幕飞 的监听任务ID列表！")
        else:
            subscribe_payload_json["data"]["taskIds"] = config.get("ordinaryroad_barrage_fly", "taskIds") 
            logger.info(subscribe_payload_json)
            asyncio.run(main(config.get("ordinaryroad_barrage_fly", "ws_ip_port")))
            
    elif platform == "talk":
        thread.join()


# 退出程序
def exit_handler(signum, frame):
    logger.info("收到信号:", signum)


if __name__ == "__main__":
    common = Common()
    config = Config(config_path)
    # 日志文件路径
    log_path = "./log/log-" + common.get_bj_time(1) + ".txt"
    # Configure_logger(log_path)

    platform = config.get("platform")

    if platform == "bilibili2":
        from typing import Optional

        # 这里填一个已登录账号的cookie。不填cookie也可以连接，但是收到弹幕的用户名会打码，UID会变成0
        SESSDATA = ""

        session: Optional[aiohttp.ClientSession] = None
    elif platform == "dy2":
        from protobuf.douyin import *

    # 按键监听相关
    do_listen_and_comment_thread = None
    stop_do_listen_and_comment_thread_event = None
    # 存储加载的模型对象
    faster_whisper_model = None
    sense_voice_model = None
    # 正在录音中 标志位
    is_recording = False
    # 聊天是否唤醒
    is_talk_awake = False

    # 待播放音频数量（在使用 音频播放器 或者 metahuman-stream等不通过AI Vtuber播放音频的对接项目时，使用此变量记录是是否还有音频没有播放完）
    wait_play_audio_num = 0

    # 信号特殊处理
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    start_server()
