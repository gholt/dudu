package dudu

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var HELP_TEXT = errors.New(strings.TrimSpace(`
Estimate disk space usage with parallelism; tries to be GNU du compatible.

Currently supports the following options:

--help

Also has the following options:

-v --verbose
--x-parallel-tasks=N
    Attempts will be made to concurrently examine files and directories up to
    the limit of N. Default for N is 100.
`))

func DUDU(args []string) error {
	cfg, items, err := parseArgs(args)
	if err != nil {
		return err
	}
	if cfg.verbosity > 1 {
		fmt.Printf("cfg is %#v\n", cfg)
		fmt.Printf("items are %#v\n", items)
	}

	counts := make(map[string]int64)
	countsLock := &sync.Mutex{}

	msgs := make(chan string, cfg.messageBuffer)
	msgsDone := make(chan struct{})
	go func() {
		for {
			msg := <-msgs
			if msg == "" {
				break
			}
			fmt.Println(msg)
		}
		close(msgsDone)
	}()

	var errCount uint32
	errs := make(chan string, cfg.errBuffer)
	errsDone := make(chan struct{})
	go func() {
		for {
			err := <-errs
			if err == "" {
				break
			}
			atomic.AddUint32(&errCount, 1)
			fmt.Println(err)
		}
		close(errsDone)
	}()

	wg := &sync.WaitGroup{}

	statTasks := make(chan *statTask, cfg.parallelTasks)
	freeStatTasks := make(chan *statTask, cfg.parallelTasks)
	for i := 0; i < cfg.parallelTasks; i++ {
		freeStatTasks <- &statTask{}
		go statter(cfg, counts, countsLock, msgs, errs, wg, statTasks, freeStatTasks)
	}

	for _, item := range items {
		fi, err := os.Lstat(item)
		if err != nil {
			errs <- fmtErr(item, err)
			continue
		}
		ct := <-freeStatTasks
		ct.item = item
		ct.fi = fi
		wg.Add(1)
		statTasks <- ct
	}

	wg.Wait()

	close(msgs)
	<-msgsDone
	close(errs)
	<-errsDone

	for _, item := range items {
		fmt.Printf("%s %#v\n", item, counts[item])
	}

	finalErrCount := atomic.LoadUint32(&errCount)
	if finalErrCount > 0 {
		return fmt.Errorf("there were %d errors", finalErrCount)
	}
	return nil
}

type config struct {
	verbosity     int
	parallelTasks int
	readdirBuffer int
	messageBuffer int
	errBuffer     int
}

func parseArgs(args []string) (*config, []string, error) {
	// TODO: Obviously a lot of this coded isn't needed right now. But it might
	// be as I work toward a useful tool. We should clean this up once we're
	// settled on a first version.
	cfg := &config{
		verbosity:     0,
		parallelTasks: 100,
		readdirBuffer: 1000,
		messageBuffer: 1000,
		errBuffer:     1000,
	}
	var items []string
	for i := 0; i < len(args); i++ {
		if args[i] == "" || args[i][0] != '-' {
			items = append(items, args[i])
			continue
		}
		if args[i] == "-" {
			items = append(items, args[i+1:]...)
			continue
		}
		var opts []string
		if args[i][1] == '-' {
			opt := args[i][2:]
			if !strings.Contains(opt, "=") {
				if opt == "preserve" {
					i++
					if len(args) <= i {
						return nil, nil, fmt.Errorf("--preserve requires a parameter")
					}
					opt += "=" + args[i]
				}
			}
			opts = append(opts, opt)
		} else {
			for _, s := range args[i][1:] {
				switch s {
				case 'a':
					opts = append(opts, "archive")
				case 'd':
					opts = append(opts, "no-dereference", "preserve=links")
				case 'L':
					opts = append(opts, "dereference")
				case 'P':
					opts = append(opts, "no-dereference")
				case 'R', 'r':
					opts = append(opts, "recursive")
				case 'v':
					opts = append(opts, "verbose")
				}
			}
		}
		var nopts []string
		for _, opt := range opts {
			if opt == "archive" {
				nopts = append(nopts, "no-dereference", "recursive", "preserve=all")
			} else {
				nopts = append(nopts, opt)
			}
		}
		opts = nopts
		for _, opt := range opts {
			var arg string
			s := strings.SplitN(opt, "=", 2)
			if len(s) > 1 {
				opt = s[0]
				arg = s[1]
			}
			switch opt {
			case "help":
				return nil, nil, HELP_TEXT
			case "verbose":
				cfg.verbosity++
			case "x-parallel-tasks":
				if arg == "" {
					return nil, nil, fmt.Errorf("--x-parallel-tasks requires a parameter")
				}
				n, err := strconv.Atoi(arg)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse number %q for --x-parallel-tasks", arg)
				}
				if n < 1 {
					n = 1
				}
				cfg.parallelTasks = n
			default:
				return nil, nil, fmt.Errorf("unknown option %q", opt)
			}
		}
	}
	return cfg, items, nil
}

type statTask struct {
	item string
	fi   os.FileInfo
}

func fmtErr(pth string, err error) string {
	rv := err.Error()
	if rv == "" {
		rv = "unknown error"
	}
	if pth != "" {
		rv = pth + ": " + rv
	}
	_, filename, line, ok := runtime.Caller(1)
	if ok {
		rv = fmt.Sprintf("%s @%s:%d", rv, path.Base(filename), line)
	}
	return rv
}

func statter(cfg *config, counts map[string]int64, countsLock *sync.Mutex, msgs chan string, errs chan string, wg *sync.WaitGroup, statTasks chan *statTask, freeStatTasks chan *statTask) {
	var localTasks []*statTask
	for {
		var item string
		var fi os.FileInfo
		if i := len(localTasks); i > 0 {
			i--
			ct := localTasks[i]
			localTasks = localTasks[:i]
			select {
			case fct := <-freeStatTasks:
				fct.item = ct.item
				fct.fi = ct.fi
				statTasks <- fct
				continue
			default:
				item = ct.item
				fi = ct.fi
			}
		} else {
			ct := <-statTasks
			item = ct.item
			fi = ct.fi
			freeStatTasks <- ct
		}
		if cfg.verbosity > 0 {
			msgs <- fmt.Sprintf("Statting %s", item)
		}
		countsLock.Lock()
		countItem := item
		for {
			counts[countItem] += fi.Size()
			if countItem == "." || countItem == "/" {
				break
			}
			countItem = path.Dir(countItem)
		}
		countsLock.Unlock()
		if fi.IsDir() {
			f, err := os.Open(item)
			if err != nil {
				errs <- fmtErr(item, err)
				wg.Done()
				continue
			}
			for {
				subfis, err := f.Readdir(cfg.readdirBuffer)
				for _, subfi := range subfis {
					subitem := path.Join(item, subfi.Name())
					wg.Add(1)
					select {
					case ct := <-freeStatTasks:
						ct.item = subitem
						ct.fi = subfi
						statTasks <- ct
					default:
						localTasks = append(localTasks, &statTask{
							item: subitem,
							fi:   subfi,
						})
					}
				}
				if err != nil {
					f.Close()
					if err != io.EOF {
						errs <- fmtErr(item, err)
					}
					break
				}
			}
		}
		wg.Done()
	}
}
