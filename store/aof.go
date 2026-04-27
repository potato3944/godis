package store

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AOF 持久化模块
type AOF struct {
	mu          sync.Mutex
	file        *os.File
	filename    string
	store       Store
}

// NewAOF 创建 AOF 实例
func NewAOF(filename string, store Store) (*AOF, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &AOF{
		file:     file,
		filename: filename,
		store:    store,
	}, nil
}

// Append 记录命令到 AOF 文件
func (a *AOF) Append(cmd string, args ...string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// RESP-like format simplification for our custom AOF
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)+1))
	sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd), cmd))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := a.file.WriteString(sb.String())
	return err
}

// Close 关闭 AOF 文件
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

// Load 从 AOF 文件加载数据重放
func (a *AOF) Load() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	file, err := os.Open(a.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 || !strings.HasPrefix(line, "*") {
			continue
		}
		
		argc, _ := strconv.Atoi(line[1:])
		var cmdArgs []string
		
		for i := 0; i < argc; i++ {
			// 读取参数长度
			lenLine, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			lenLine = strings.TrimSpace(lenLine)
			if !strings.HasPrefix(lenLine, "$") {
				break
			}
			
			// 读取参数值
			argLine, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			cmdArgs = append(cmdArgs, strings.TrimSpace(argLine))
		}
		
		if len(cmdArgs) > 0 {
			cmd := strings.ToUpper(cmdArgs[0])
			switch cmd {
			case "SET":
				if len(cmdArgs) >= 3 {
					a.store.Set(cmdArgs[1], cmdArgs[2])
				}
			case "DEL":
				if len(cmdArgs) >= 2 {
					a.store.Delete(cmdArgs[1])
				}
			case "EXPIRE":
				if len(cmdArgs) >= 3 {
					ttl, _ := strconv.Atoi(cmdArgs[2])
					a.store.SetExpire(cmdArgs[1], time.Duration(ttl)*time.Millisecond)
				}
			}
		}
	}
	
	log.Printf("AOF loading complete.")
	return nil
}
