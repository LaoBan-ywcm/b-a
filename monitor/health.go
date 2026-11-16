package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// telegramIncident 只保存当前通知中断的起点和失败次数，不积压失败消息正文。
type telegramIncident struct {
	since    time.Time
	failures uint64
}

// marketIncident 按故障范围记录连续失败；成功发送后只标记事件，不触碰合约上涨提醒冷却。
type marketIncident struct {
	since    time.Time
	notified bool
}

// healthSummary 只保存有限条可读范围和事件总数；通知中断期间不断变化的已恢复事件不能形成无限队列。
// 当前仍失败的事件另行记录，最多为本轮 TopN 个局部范围和一个全局范围。
type healthSummary struct {
	entries []string
	count   int
}

func (s *healthSummary) add(scope string, since time.Time) {
	s.count++
	if len(s.entries) == 6 {
		return
	}
	name := "全局行情获取（部分已有完整数据的合约可能仍可检查）"
	if scope != "" {
		name = "局部合约 " + shortScope(scope)
	}
	s.entries = append(s.entries, name+"；开始时间："+since.Format(time.RFC3339Nano))
}

func shortScope(scope string) string {
	runes := []rune(scope)
	if len(runes) > 64 {
		return string(runes[:64]) + "…"
	}
	return scope
}

func (s healthSummary) text() string {
	text := strings.Join(s.entries, "\n")
	if s.count > len(s.entries) {
		text += fmt.Sprintf("\n另有 %d 个范围，完整诊断见日志", s.count-len(s.entries))
	}
	return text
}

func (m *Monitor) finishHealth(ctx context.Context, result *Result) error {
	if m.marketIncidents == nil {
		m.marketIncidents = make(map[string]*marketIncident)
	}
	current := make(map[string]bool)
	for _, failure := range result.MarketFailures {
		if failure.Global {
			current[""] = true
		} else {
			current[failure.Symbol] = true
		}
	}
	if result.MarketStatus == MarketFailed {
		current[""] = true
	}
	// 全局失败时未返回的局部候选只是未知，不能误报其恢复；全局事件覆盖本轮新出现的局部错误。
	// 只有能完整取得全局范围后，才根据本轮局部失败集合判断覆盖是否恢复。
	for scope, incident := range m.marketIncidents {
		if current[scope] || (scope != "" && current[""]) {
			continue
		}
		if incident.notified {
			m.marketRecoveries.add(scope, incident.since)
		}
		delete(m.marketIncidents, scope)
	}
	for scope := range current {
		if scope != "" && current[""] {
			continue
		}
		if m.marketIncidents[scope] == nil {
			m.marketIncidents[scope] = &marketIncident{since: m.now()}
		}
	}
	// 本轮上涨发送已经失败时只更新健康状态，不紧接着再发一轮健康重试。
	if m.telegramFailedThisCheck {
		return nil
	}
	var due []string
	for scope, incident := range m.marketIncidents {
		if !incident.notified && m.now().Sub(incident.since) > time.Minute {
			due = append(due, scope)
		}
	}
	sort.Strings(due)
	var failures healthSummary
	for _, scope := range due {
		failures.add(scope, m.marketIncidents[scope].since)
	}
	var sections []string
	if incident := m.telegramIncident; incident != nil {
		sections = append(sections, fmt.Sprintf("Telegram 通知恢复\n中断开始：%s\n中断时长：%s\n失败投递：%d 次\n期间旧行情通知已丢弃，按当前行情继续检查。", incident.since.Format(time.RFC3339Nano), m.now().Sub(incident.since), incident.failures))
	}
	if failures.count > 0 {
		sections = append(sections, "行情故障\n范围：\n"+failures.text())
	}
	if m.marketRecoveries.count > 0 {
		sections = append(sections, "行情恢复\n范围：\n"+m.marketRecoveries.text()+"\n此前事件已恢复：当时已能正常检查，或相关合约已不再需要检查。")
		if current[""] {
			sections = append(sections, "当前存在全局行情故障，不能确认全部监控范围。")
		} else if len(current) > 0 {
			sections = append(sections, "当前仍有局部故障，其他范围继续监控。")
		} else {
			sections = append(sections, "当前行情检查正常。")
		}
	}
	if len(sections) == 0 {
		return nil
	}
	text := strings.Join(sections, "\n\n") + "\n检查时间：" + m.now().Format(time.RFC3339Nano)
	if err := m.sendTelegram(ctx, text, m.now().Add(time.Minute)); err != nil {
		return &CheckError{Service: "Telegram", Operation: "health", Err: err}
	}
	for _, scope := range due {
		m.marketIncidents[scope].notified = true
	}
	m.marketRecoveries = healthSummary{}
	if m.telegramIncident != nil {
		m.logger.Printf("Telegram 通知恢复：已确认恢复汇报送达")
		m.telegramIncident = nil
	}
	return nil
}
