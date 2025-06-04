import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
ffmpeg.setFfmpegPath(ffmpegInstaller.path);
import pDebounce from 'p-debounce';
import sharp from 'sharp';
import Log from 'debug-level';
import { demux } from './LibavDemuxer.js';
import { setTimeout as delay } from 'node:timers/promises';
import { PassThrough, type Readable } from "node:stream";
import { VideoStream } from './VideoStream.js';
import { AudioStream } from './AudioStream.js';
import { isFiniteNonZero } from '../utils.js';
import { AVCodecID } from './LibavCodecId.js';
import { createDecoder } from './LibavDecoder.js';

import LibAV from '@lng2004/libav.js-variant-webcodecs-avf-with-decoders';
import type { SupportedVideoCodec } from '../utils.js';
import type { MediaUdp, Streamer } from '../client/index.js';

export interface Controller {
    mute(): void;
    unmute(): void;
    isMuted(): boolean;
}

export type EncoderOptions = {
    /**
     * Disable video transcoding
     * If enabled, all video related settings have no effects, and the input
     * video stream is used as-is. Video playback speed modification will also be disabled.
     */
    noTranscoding: boolean,

    /**
     * Video width
     */
    width: number,

    /**
     * Video height
     */
    height: number,

    /**
     * Video frame rate
     */
    frameRate?: number,

    /**
     * Video codec
     */
    videoCodec: SupportedVideoCodec,

    /**
     * Video average bitrate in kbps
     */
    bitrateVideo: number,

    /**
     * Video max bitrate in kbps
     */
    bitrateVideoMax: number,

    /**
     * Audio bitrate in kbps
     */
    bitrateAudio: number,

    /**
     * Enable audio output
     */
    includeAudio: boolean,

    /**
     * Playback speed. Default is 1.0 (normal speed).
     * Affects both video (if transcoded) and audio (if included).
     * For example, 2.0 is 2x speed, 0.5 is half speed.
     */
    playbackSpeed: number,

    /**
     * Enable hardware accelerated decoding
     */
    hardwareAcceleratedDecoding: boolean,

    /**
     * Add some options to minimize latency
     */
    minimizeLatency: boolean,

    /**
     * Preset for x264 and x265
     */
    h26xPreset: "ultrafast" | "superfast" | "veryfast" | "faster" | "fast" | "medium" | "slow" | "slower" | "veryslow" | "placebo",

    /**
     * Custom headers for HTTP requests
     */
    customHeaders: Record<string, string>,

    /**
     * Custom ffmpeg flags/options to pass directly to ffmpeg
     * These will be added to the command after other options
     */
    customFfmpegFlags: string[]
}

/**
 * Helper function to generate the atempo filter string for FFmpeg.
 * Handles chaining for speeds outside the 0.5-100 range.
 * @param speed Desired playback speed.
 * @returns FFmpeg audio filter string for atempo.
 */
function generateAtempoFilterString(speed: number): string {
    if (speed <= 0) {
        // Or throw an error, depending on desired handling for invalid speeds
        Log("prepareStream:atempo").warn(`Invalid playback speed ${speed} requested. Defaulting to 1.0.`);
        return "";
    }
    if (Math.abs(speed - 1.0) < 1e-4) return ""; // Effectively 1.0, no filter needed

    const filters: string[] = [];
    let currentSpeedFactor = speed;

    // Handle speeds greater than 1.0 by potentially chaining atempo=100.0 or smaller factors
    // atempo max is 100.0. For simplicity and common use cases (e.g. up to 4x),
    // we can also chain factors of 2.0 for robustness, though a single atempo up to 100 is valid.
    // Here, we'll decompose into factors ensuring each is within [0.5, 100.0].
    if (currentSpeedFactor > 1.0) {
        while (currentSpeedFactor > 100.0 && filters.length < 10) { // Cap iterations to prevent infinite loops
            filters.push("atempo=100.0");
            currentSpeedFactor /= 100.0;
        }
    }
    // Handle speeds less than 1.0 by potentially chaining atempo=0.5
    else if (currentSpeedFactor < 1.0) {
        while (currentSpeedFactor < 0.5 && filters.length < 10) { // Cap iterations
            filters.push("atempo=0.5");
            currentSpeedFactor /= 0.5;
        }
    }

    // Add the final tempo adjustment if it's not 1.0 (after decomposition)
    // and it's within the valid single atempo range.
    if (Math.abs(currentSpeedFactor - 1.0) > 1e-4) {
        const finalTempoValue = Math.max(0.5, Math.min(100.0, currentSpeedFactor));
         if (Math.abs(finalTempoValue - 1.0) > 1e-4) { // Avoid adding atempo=1.0
            filters.push(`atempo=${finalTempoValue.toFixed(4)}`);
        }
    }
    
    return filters.join(',');
}


export function prepareStream(
    input: string | Readable,
    options: Partial<EncoderOptions> = {},
    cancelSignal?: AbortSignal
) {
    cancelSignal?.throwIfAborted();
    const defaultOptions = {
        noTranscoding: false,
        width: -2,
        height: -2,
        frameRate: undefined,
        videoCodec: "H264",
        bitrateVideo: 5000,
        bitrateVideoMax: 7000,
        bitrateAudio: 128,
        includeAudio: true,
        playbackSpeed: 1.0, // Default normal speed
        hardwareAcceleratedDecoding: false,
        minimizeLatency: false,
        h26xPreset: "ultrafast",
        customHeaders: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.3",
            "Connection": "keep-alive",
        },
        customFfmpegFlags: []
    } satisfies EncoderOptions;

    function mergeOptions(opts: Partial<EncoderOptions>) {
        const merged = {
            noTranscoding:
                opts.noTranscoding ?? defaultOptions.noTranscoding,

            width:
                isFiniteNonZero(opts.width) ? Math.round(opts.width) : defaultOptions.width,

            height:
                isFiniteNonZero(opts.height) ? Math.round(opts.height) : defaultOptions.height,

            frameRate:
                isFiniteNonZero(opts.frameRate) && opts.frameRate > 0
                    ? opts.frameRate
                    : defaultOptions.frameRate,

            videoCodec:
                opts.videoCodec ?? defaultOptions.videoCodec,

            bitrateVideo:
                isFiniteNonZero(opts.bitrateVideo) && opts.bitrateVideo > 0
                    ? Math.round(opts.bitrateVideo)
                    : defaultOptions.bitrateVideo,

            bitrateVideoMax:
                isFiniteNonZero(opts.bitrateVideoMax) && opts.bitrateVideoMax > 0
                    ? Math.round(opts.bitrateVideoMax)
                    : defaultOptions.bitrateVideoMax,

            bitrateAudio:
                isFiniteNonZero(opts.bitrateAudio) && opts.bitrateAudio > 0
                    ? Math.round(opts.bitrateAudio)
                    : defaultOptions.bitrateAudio,

            includeAudio:
                opts.includeAudio ?? defaultOptions.includeAudio,

            playbackSpeed:
                isFiniteNonZero(opts.playbackSpeed) && opts.playbackSpeed > 0
                    ? opts.playbackSpeed
                    : defaultOptions.playbackSpeed,
            
            hardwareAcceleratedDecoding:
                opts.hardwareAcceleratedDecoding ?? defaultOptions.hardwareAcceleratedDecoding,

            minimizeLatency:
                opts.minimizeLatency ?? defaultOptions.minimizeLatency,

            h26xPreset:
                opts.h26xPreset ?? defaultOptions.h26xPreset,

            customHeaders: {
                ...defaultOptions.customHeaders, ...opts.customHeaders
            },

            customFfmpegFlags:
                opts.customFfmpegFlags ?? defaultOptions.customFfmpegFlags
        } satisfies EncoderOptions;

        if (merged.playbackSpeed <= 0) {
            Log("prepareStream:options").warn(`Invalid playbackSpeed ${merged.playbackSpeed}. Resetting to 1.0.`);
            merged.playbackSpeed = 1.0;
        }
        return merged;
    }

    const mergedOptions = mergeOptions(options);

    let isHttpUrl = false;
    let isHls = false;

    if (typeof input === "string") {
        isHttpUrl = input.startsWith('http') || input.startsWith('https');
        isHls = input.includes('m3u');
    }

    const output = new PassThrough();

    // command creation
    const command = ffmpeg(input)
        .addOption('-loglevel', '0')

    // input options
    const { hardwareAcceleratedDecoding, minimizeLatency, customHeaders } = mergedOptions;
    if (hardwareAcceleratedDecoding)
        command.inputOption('-hwaccel', 'auto');

    if (minimizeLatency) {
        command.addOptions([
            '-fflags nobuffer',
            '-analyzeduration 0'
        ])
    }

    if (isHttpUrl) {
        command.inputOption('-headers',
            Object.entries(customHeaders).map(([k, v]) => `${k}: ${v}`).join("\r\n")
        );
        if (!isHls) {
            command.inputOptions([
                '-reconnect 1',
                '-reconnect_at_eof 1',
                '-reconnect_streamed 1',
                '-reconnect_delay_max 4294'
            ]);
        }
    }

    // general output options
    command
        .output(output)
        .outputFormat("matroska");

    // video setup
    const {
        noTranscoding, width, height, frameRate, bitrateVideo, bitrateVideoMax, videoCodec, h26xPreset,
        playbackSpeed
    } = mergedOptions;
    command.addOutputOption("-map 0:v");

    if (noTranscoding)
    {
        command.videoCodec("copy");
        if (Math.abs(playbackSpeed - 1.0) > 1e-4) {
            Log("prepareStream:video").warn("Video playback speed modification is ignored when noTranscoding is true.");
        }
    }
    else
    {
        const videoFilters: string[] = [];
        videoFilters.push(`scale=${width}:${height}`); // Scaling filter

        // Add setpts filter for video speed if playbackSpeed is not 1.0
        if (Math.abs(playbackSpeed - 1.0) > 1e-4 && playbackSpeed > 0) {
            videoFilters.push(`setpts=PTS/${playbackSpeed.toFixed(4)}`);
        }
        
        if (videoFilters.length > 0) {
            command.videoFilter(videoFilters.join(','));
        }

        if (frameRate)
            command.fpsOutput(frameRate);

        command.addOutputOption([
            "-b:v", `${bitrateVideo}k`,
            "-maxrate:v", `${bitrateVideoMax}k`,
            "-bf", "0",
            "-pix_fmt", "yuv420p",
            "-force_key_frames", "expr:gte(t,n_forced*1)"
        ]);

        switch (videoCodec) {
            case 'AV1':
                command.videoCodec("libsvtav1");
                break;
            case 'VP8':
                command.videoCodec("libvpx").outputOption('-deadline', 'realtime');
                break;
            case 'VP9':
                command.videoCodec("libvpx-vp9").outputOption('-deadline', 'realtime');
                break;
            case 'H264':
                command.videoCodec("libx264").outputOptions([
                    '-tune zerolatency',
                    `-preset ${h26xPreset}`,
                    '-profile:v baseline',
                ]);
                break;
            case 'H265':
                command.videoCodec("libx265").outputOptions([
                    '-tune zerolatency',
                    `-preset ${h26xPreset}`,
                    '-profile:v main',
                ]);
                break;
        }
    }

    // audio setup
    const { includeAudio, bitrateAudio } = mergedOptions;
    if (includeAudio) {
        command
            .addOutputOption("-map 0:a?")
            .audioChannels(2)
            .addOutputOption("-lfe_mix_level 1")
            .audioFrequency(48000)
            .audioCodec("libopus"); // Set codec before custom filters if filters expect PCM

        // Add atempo filter for audio speed if playbackSpeed is not 1.0
        if (Math.abs(playbackSpeed - 1.0) > 1e-4 && playbackSpeed > 0) {
            const atempoFilter = generateAtempoFilterString(playbackSpeed);
            if (atempoFilter) {
                command.audioFilter(atempoFilter);
            }
        }
        
        command.audioBitrate(`${bitrateAudio}k`); // Set bitrate after filters
    }


    // Add custom ffmpeg flags
    if (mergedOptions.customFfmpegFlags && mergedOptions.customFfmpegFlags.length > 0) {
        command.addOptions(mergedOptions.customFfmpegFlags);
    }

    // exit handling
    const promise = new Promise<void>((resolve, reject) => {
        command.on("error", (err) => {
            if (cancelSignal?.aborted)
                reject(cancelSignal.reason);
            else
                reject(err);
        });
        command.on("end", () => resolve());
    })
    promise.catch(() => {}); // Prevent unhandled rejection if caller doesn't catch
    cancelSignal?.addEventListener("abort", () => command.kill("SIGTERM"), { once: true });
    command.run();

    return { command, output, promise }
}

export type PlayStreamOptions = {
    /**
     * Set stream type as "Go Live" or camera stream
     */
    type: "go-live" | "camera",

    /**
     * Override video width sent to Discord.
     * DO NOT SPECIFY UNLESS YOU KNOW WHAT YOU'RE DOING!
     */
    width: number,

    /**
     * Override video height sent to Discord.
     * DO NOT SPECIFY UNLESS YOU KNOW WHAT YOU'RE DOING!
     */
    height: number,

    /**
     * Override video frame rate sent to Discord.
     * DO NOT SPECIFY UNLESS YOU KNOW WHAT YOU'RE DOING!
     */
    frameRate: number,

    /**
     * Same as ffmpeg's `readrate_initial_burst` command line flag
     * See https://ffmpeg.org/ffmpeg.html#:~:text=%2Dreadrate_initial_burst
     */
    readrateInitialBurst: number | undefined,

    /**
     * Enable stream preview from input stream (experimental)
     */
    streamPreview: boolean,
}

export async function playStream(
    input: Readable, streamer: Streamer,
    options: Partial<PlayStreamOptions> = {},
    cancelSignal?: AbortSignal
)
{
    const logger = new Log("playStream");
    cancelSignal?.throwIfAborted();
    if (!streamer.voiceConnection)
        throw new Error("Bot is not connected to a voice channel");

    logger.debug("Initializing demuxer");
    const { video, audio } = await demux(input);
    cancelSignal?.throwIfAborted();

    if (!video)
        throw new Error("No video stream in media");

    const cleanupFuncs: (() => unknown)[] = [];
    const videoCodecMap: Record<number, SupportedVideoCodec> = {
        [AVCodecID.AV_CODEC_ID_H264]: "H264",
        [AVCodecID.AV_CODEC_ID_H265]: "H265",
        [AVCodecID.AV_CODEC_ID_VP8]: "VP8",
        [AVCodecID.AV_CODEC_ID_VP9]: "VP9",
        [AVCodecID.AV_CODEC_ID_AV1]: "AV1"
    }
    const defaultOptions = {
        type: "go-live",
        width: video.width,
        height: video.height,
        frameRate: video.framerate_num / video.framerate_den,
        readrateInitialBurst: undefined,
        streamPreview: false,
    } satisfies PlayStreamOptions;

    function mergeOptions(opts: Partial<PlayStreamOptions>)
    {
        return {
            type:
                opts.type ?? defaultOptions.type,

            width:
                isFiniteNonZero(opts.width) && opts.width > 0
                    ? Math.round(opts.width)
                    : defaultOptions.width,

            height:
                isFiniteNonZero(opts.height) && opts.height > 0
                    ? Math.round(opts.height)
                    : defaultOptions.height,

            frameRate: Math.round(
                isFiniteNonZero(opts.frameRate) && opts.frameRate > 0
                    ? Math.round(opts.frameRate)
                    : defaultOptions.frameRate
            ),

            readrateInitialBurst:
                isFiniteNonZero(opts.readrateInitialBurst) && opts.readrateInitialBurst > 0
                    ? opts.readrateInitialBurst
                    : defaultOptions.readrateInitialBurst,

            streamPreview:
                opts.streamPreview ?? defaultOptions.streamPreview,
        } satisfies PlayStreamOptions
    }

    const mergedOptions = mergeOptions(options);
    logger.debug({ options: mergedOptions }, "Merged options");

    let udp: MediaUdp;
    let stopStream: () => unknown;
    if (mergedOptions.type === "go-live")
    {
        udp = await streamer.createStream();
        stopStream = () => streamer.stopStream();
    }
    else
    {
        udp = streamer.voiceConnection.udp;
        streamer.signalVideo(true);
        stopStream = () => streamer.signalVideo(false);
    }
    udp.setPacketizer(videoCodecMap[video.codec]);
    udp.mediaConnection.setSpeaking(true);
    udp.mediaConnection.setVideoAttributes(true, {
        width: mergedOptions.width,
        height: mergedOptions.height,
        fps: mergedOptions.frameRate
    });

    const vStream = new VideoStream(udp);
    video.stream.pipe(vStream);

    let aStream: AudioStream | undefined; // Declare the audio stream instance

    if (audio)
    {
        aStream = new AudioStream(udp);
        audio.stream.pipe(aStream);
        vStream.syncStream = aStream;
        aStream.syncStream = vStream;

        const burstTime = mergedOptions.readrateInitialBurst;
        if (typeof burstTime === "number")
        {
            vStream.sync = aStream.sync = false;
            vStream.noSleep = aStream.noSleep = true;
            const stopBurst = (pts: number) => {
                if (pts < burstTime * 1000)
                    return;
                // biome-ignore lint/style/noNonNullAssertion:
                vStream.sync = aStream!.sync = true;
                // biome-ignore lint/style/noNonNullAssertion:
                vStream.noSleep = vStream!.sync = false; // This was vStream.sync, should be vStream.noSleep
                vStream.off("pts", stopBurst);
            }
            vStream.on("pts", stopBurst);
        }
    }
    if (mergedOptions.streamPreview && mergedOptions.type === "go-live")
    {
        (async () => {
            const logger = new Log("playStream:preview");
            logger.debug("Initializing decoder for stream preview");
            const decoder = await createDecoder(video.codec, video.codecpar);
            if (!decoder)
            {
                logger.warn("Failed to initialize decoder. Stream preview will be disabled");
                return;
            }
            cleanupFuncs.push(() => {
                logger.debug("Freeing decoder");
                decoder.free();
            });
            const updatePreview = pDebounce.promise(async (packet: LibAV.Packet) => {
                if (!(packet.flags !== undefined && packet.flags & LibAV.AV_PKT_FLAG_KEY))
                    return;
                const decodeStart = performance.now();
                const [frame] = await decoder.decode([packet]).catch(() => []);
                if (!frame)
                    return;
                const decodeEnd = performance.now();
                logger.debug(`Decoding a frame took ${decodeEnd - decodeStart}ms`);

                return sharp(frame.data, {
                    raw: {
                        width: frame.width ?? 0,
                        height: frame.height ?? 0,
                        channels: 4
                    }
                })
                .resize(1024, 576, { fit: "inside" })
                .jpeg()
                .toBuffer()
                .then(image => streamer.setStreamPreview(image))
                .catch(() => {});
            });
            video.stream.on("data", updatePreview);
            cleanupFuncs.push(() => video.stream.off("data", updatePreview));
        })();
    }

    const streamPromise = new Promise<void>((resolve, reject) => {
        cleanupFuncs.push(() => {
            stopStream();
            udp.mediaConnection.setSpeaking(false);
            udp.mediaConnection.setVideoAttributes(false);
        });
        let cleanedUp = false;
        const cleanup = () => {
            if (cleanedUp)
                return;
            cleanedUp = true;
            for (const f of cleanupFuncs)
                f();
        }
        cancelSignal?.addEventListener("abort", () => {
            cleanup();
            reject(cancelSignal.reason);
        }, { once: true })
        vStream.once("finish", () => {
            if (cancelSignal?.aborted)
                return;
            cleanup();
            resolve();
        });
    }).catch(() => {}); // Prevent unhandled rejection

    // Return the promise and the controller
    return {
        controller: {
            mute() {
                aStream?.mute();
            },
            unmute() {
                aStream?.unmute();
            },
            isMuted() {
                return !!aStream?.isMuted();
            }
        } satisfies Controller,
        done: streamPromise
    };
}
