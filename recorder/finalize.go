package recorder

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"recd/config"
)

var finalizeMediaFiles = runFFmpegAndProbe

func finalizeRecording(app *config.AppContext, username, videoPath, audioPath, outputPath string, result *Result) {
	videoSize := fileSize(videoPath)
	audioSize := fileSize(audioPath)
	if videoSize == 0 || audioSize == 0 {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf(
			"no mergeable media: video=%d bytes audio=%d bytes",
			videoSize,
			audioSize,
		))
		return
	}

	partial, err := os.CreateTemp(
		filepath.Dir(outputPath),
		"."+filepath.Base(outputPath)+".*.part",
	)
	if err != nil {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("create partial output: %w", err))
		return
	}
	partialPath := partial.Name()
	if err := partial.Close(); err != nil {
		_ = os.Remove(partialPath)
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("close partial output: %w", err))
		return
	}

	app.Logger.Info("merging video and audio",
		"username", username,
		"output", outputPath,
		"video_size", videoSize,
		"audio_size", audioSize,
	)
	if err := finalizeMediaFiles(app, username, videoPath, audioPath, partialPath); err != nil {
		_ = os.Remove(partialPath)
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("finalize media: %w", err))
		return
	}
	if fileSize(partialPath) == 0 {
		_ = os.Remove(partialPath)
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("validated partial output is empty"))
		return
	}
	if err := os.Rename(partialPath, outputPath); err != nil {
		_ = os.Remove(partialPath)
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("publish output: %w", err))
		return
	}

	result.Filesize = fileSize(outputPath)
	if result.Filesize == 0 {
		result.Status = StatusError
		result.Err = errors.Join(result.Err, fmt.Errorf("published output is empty"))
		return
	}
	removeTempFile(app, username, "video", videoPath)
	removeTempFile(app, username, "audio", audioPath)
}

func runFFmpegAndProbe(_ *config.AppContext, _ string, videoPath, audioPath, outputPath string) error {
	merge := exec.Command(
		"ffmpeg",
		"-hide_banner",
		"-loglevel", "warning",
		"-copyts",
		"-start_at_zero",
		"-i", videoPath,
		"-i", audioPath,
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-c", "copy",
		"-f", "matroska",
		"-y",
		outputPath,
	)
	merge.Stderr = os.Stderr
	if err := merge.Run(); err != nil {
		return fmt.Errorf("ffmpeg: %w", err)
	}

	return probeMediaFile(outputPath)
}

func probeMediaFile(path string) error {
	probe := exec.Command(
		"ffprobe",
		"-v", "error",
		"-show_entries", "stream=codec_type:format=duration",
		"-of", "json",
		path,
	)
	output, err := probe.Output()
	if err != nil {
		return fmt.Errorf("ffprobe: %w", err)
	}
	var metadata struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(output, &metadata); err != nil {
		return fmt.Errorf("decode ffprobe output: %w", err)
	}
	var video, audio bool
	for _, stream := range metadata.Streams {
		video = video || stream.CodecType == "video"
		audio = audio || stream.CodecType == "audio"
	}
	if !video || !audio {
		return fmt.Errorf("ffprobe found video=%t audio=%t", video, audio)
	}
	duration, err := strconv.ParseFloat(metadata.Format.Duration, 64)
	if err != nil || duration <= 0 {
		return fmt.Errorf("ffprobe returned invalid duration %q", metadata.Format.Duration)
	}
	return nil
}

func removeTempFile(app *config.AppContext, username, track, path string) {
	if err := os.Remove(path); err != nil {
		app.Logger.Warn("failed to remove temp file",
			"username", username,
			"track", track,
			"path", path,
			"error", err,
		)
	}
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
