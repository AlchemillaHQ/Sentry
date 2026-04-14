package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlchemillaHQ/Difuse-B2BUA/b2bua"
	"github.com/AlchemillaHQ/Difuse-B2BUA/config"
	"github.com/c-bata/go-prompt"
	"github.com/cloudwebrtc/go-sip-ua/pkg/utils"
	"github.com/ghettovoice/gosip/log"
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "users", Description: "Show sip accounts"},
		{Text: "onlines", Description: "Show online sip devices"},
		{Text: "calls", Description: "Show active calls"},
		{Text: "set debug on", Description: "Show debug msg in console"},
		{Text: "set debug off", Description: "Turn off debug msg in console"},
		{Text: "show loggers", Description: "Print Loggers"},
		{Text: "exit", Description: "Exit"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func usage() {
	fmt.Fprintf(os.Stderr, `go pbx version: go-pbx/1.10.0
Usage: server [-nc]

Options:
`)
	flag.PrintDefaults()
}

func consoleLoop(b2bua *b2bua.B2BUA) {

	fmt.Println("Please select command.")
	for {
		t := prompt.Input("CLI> ", completer,
			prompt.OptionTitle("GO B2BUA 1.0.0"),
			prompt.OptionHistory([]string{"calls", "users", "onlines"}),
			prompt.OptionPrefixTextColor(prompt.Yellow),
			prompt.OptionPreviewSuggestionTextColor(prompt.Blue),
			prompt.OptionSelectedSuggestionBGColor(prompt.LightGray),
			prompt.OptionSuggestionBGColor(prompt.DarkGray))

		switch t {
		case "show loggers":
			loggers := utils.GetLoggers()
			for prefix, log := range loggers {
				fmt.Printf("%v => %v\n", prefix, log.Level())
			}
		case "set debug on":
			b2bua.SetLogLevel(log.DebugLevel)
			fmt.Printf("Set Log level to debug\n")
		case "set debug off":
			b2bua.SetLogLevel(log.InfoLevel)
			fmt.Printf("Set Log level to info\n")
		case "users":
			fallthrough
		case "ul": /* user list*/
			accounts := b2bua.GetAccounts()
			if len(accounts) > 0 {
				fmt.Printf("Users:\n")
				fmt.Printf("Username \t Password\n")
				for user, pass := range accounts {
					fmt.Printf("%v \t\t %v\n", user, pass)
				}
			} else {
				fmt.Printf("No users\n")
			}
		case "calls":
			fallthrough
		case "cl": /* call list*/
			calls := b2bua.Calls()
			if len(calls) > 0 {
				fmt.Printf("Calls:\n")
				for _, call := range calls {
					fmt.Printf("%v:\n", call.ToString())
				}
			} else {
				fmt.Printf("No active calls\n")
			}
		case "onlines":
			fallthrough
		case "rr": /* register records*/
			aors := b2bua.GetRegistry().GetAllContacts()
			if len(aors) > 0 {
				for aor, instances := range aors {
					fmt.Printf("AOR: %v:\n", aor)
					for _, instance := range instances {
						fmt.Printf("\t%v, Expires: %d, Source: %v, Transport: %v\n",
							(*instance).UserAgent,
							(*instance).RegExpires,
							(*instance).Source,
							(*instance).Transport)
					}
				}
			} else {
				fmt.Printf("No online devices\n")
			}
		case "pr": /* pn records*/
			pnrs := b2bua.GetRFC8599().PNRecords()
			if len(pnrs) > 0 {
				fmt.Printf("PN Records:\n")
				for pn, aor := range pnrs {
					fmt.Printf("AOR: %v => pn-provider=%v, pn-param=%v, pn-prid=%v\n", aor, pn.Provider, pn.Param, pn.PRID)
				}
			} else {
				fmt.Printf("No pn records\n")
			}
		case "exit":
			fmt.Println("Exit now.")
			b2bua.Shutdown()
			return
		}
	}
}

func main() {
	noconsole := false
	h := false
	configPath := "config.yaml"
	flag.BoolVar(&h, "h", false, "this help")
	flag.BoolVar(&noconsole, "nc", false, "no console mode")
	flag.StringVar(&configPath, "config", configPath, "path to config file")
	flag.Usage = usage

	flag.Parse()

	if h {
		flag.Usage()
		return
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config %q: %v\n", configPath, err)
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		fmt.Printf("Start pprof on %s\n", cfg.Pprof.Addr)
		if err := http.ListenAndServe(cfg.Pprof.Addr, nil); err != nil {
			fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
		}
	}()

	b := b2bua.NewB2BUA(cfg)

	if !noconsole {
		consoleLoop(b)
		return
	}

	<-stop
	b.Shutdown()
}
