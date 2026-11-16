package monitor

import (
	"context"
	"errors"
	"time"
)

// Run 启动即检查，后续按轮次开始时间调度，始终串行执行。
// 慢轮次错过的检查时刻直接跳过，避免恢复后补跑过时行情；普通 API 失败只记录并继续。
// 冷却持久化失败必须终止服务，让运维修复后重启，防止在无法保存状态时继续发送。
func (m *Monitor) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		started := m.now()
		result, err := m.Check(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			m.logger.Printf("检查失败：%v", err)
			var stateErr *StateError
			if errors.As(err, &stateErr) {
				return err
			}
		}
		m.logger.Printf("检查完成：候选 %d，成功通知 %d，诊断 %d", len(result.Alerts), len(result.Sent), len(result.Diagnostics))
		next := started.Add(m.config.CheckInterval)
		remaining := next.Sub(m.now())
		if remaining < 0 {
			// 跳过已错过的周期，最多等待一个周期；负值取模避免循环补算长时间中断。
			remaining = m.config.CheckInterval + remaining%m.config.CheckInterval
		}
		if err := m.wait(ctx, remaining); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
