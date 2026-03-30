package logger

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// getVerbosity 从环境变量中读取日志级别。
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var mu sync.Mutex

// LOGinit 初始化日志模块的起始时间和日志级别。
func LOGinit() {
	mu.Lock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	mu.Unlock()
}

// DEBUG 输出通用调试日志，并附带调用位置，便于排查问题。
func DEBUG(topic logTopic, format string, a ...interface{}) {
	_, file, lineNo, ok := runtime.Caller(1)
	if !ok {
		log.Println("runtime.Caller() failed")
	}

	fileName := path.Base(file)
	if 3 >= 1 {
		mu.Lock()
		prefix := fmt.Sprintf("%v ", string(topic))
		format = prefix + fileName + ":" + strconv.Itoa(lineNo) + ": " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}

// DEBUG_RAFT 用于输出 Raft 相关的调试日志。
func DEBUG_RAFT(topic logTopic, format string, a ...interface{}) {
	_, file, lineNo, ok := runtime.Caller(1)
	if !ok {
		log.Println("runtime.Caller() failed")
	}

	fileName := path.Base(file)
	if 0 >= 1 {
		mu.Lock()
		prefix := fmt.Sprintf("%v ", string(topic))
		format = prefix + fileName + ":" + strconv.Itoa(lineNo) + ": " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}
