package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// StateError 表示冷却状态不可安全恢复或保存；持续服务必须停止，避免状态丢失后连续重复通知。
// 错误不附带原始文件内容，防止损坏状态里的任意文本进入日志。
type StateError struct{ Operation string }

func (e *StateError) Error() string {
	return fmt.Sprintf("冷却状态%s失败，请检查 STATE_PATH 和目录权限", e.Operation)
}

type cooldownState struct {
	Version  int                  `json:"version"`
	LastSent map[string]time.Time `json:"last_sent"`
}

func loadState(path string) (map[string]time.Time, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return make(map[string]time.Time), nil
	}
	if err != nil {
		return nil, &StateError{Operation: "读取"}
	}
	var state cooldownState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || state.Version != 1 || state.LastSent == nil {
		return nil, &StateError{Operation: "解析"}
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, &StateError{Operation: "解析"}
	}
	for symbol, at := range state.LastSent {
		if symbol == "" || at.IsZero() {
			return nil, &StateError{Operation: "解析"}
		}
	}
	return state.LastSent, nil
}

// 单实例使用同目录临时文件：先完整写入并同步，再原子替换正式状态，最后同步目录项。
// 若写入或重命名失败，旧正式文件仍保持可读；启动只读正式文件，不采用不完整的临时文件。
// Telegram 发送与文件写入无法成为同一事务；发送后崩溃仍可能重复，不能承诺 exactly-once。
func (m *Monitor) saveState() error {
	failed := func() error { return &StateError{Operation: "写入"} }
	path := m.config.StatePath
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return failed()
	}
	data, err := json.Marshal(cooldownState{Version: 1, LastSent: m.lastSent})
	if err != nil {
		return failed()
	}
	// 固定临时名称仅适用于文档约定的单进程单实例，残留临时文件会在下一次保存时覆盖。
	file, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return failed()
	}
	if _, err = file.Write(data); err != nil {
		file.Close()
		return failed()
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return failed()
	}
	if err = file.Close(); err != nil {
		return failed()
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return failed()
	}
	directory, err := os.Open(dir)
	if err != nil {
		return failed()
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return failed()
	}
	return nil
}
