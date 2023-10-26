package main

import (
	"badgerlit/sdk"
	"badgerlit/storage/badger"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Bofry/config"
	"github.com/tidwall/resp"
)

var (
	DefaultConfigFile = "config.yaml"

	configFile = flag.String("config", DefaultConfigFile, "specified config file. Default: config.yaml")
)

func main() {
	flag.Parse()

	var (
		db sdk.Storage
	)

	conf := sdk.Config{
		ListenAddress:      sdk.DefaultListenAddress,
		Engine:             sdk.DefaultEngine,
		DataPath:           sdk.DefaultDataPath,
		KeyDiscardInterval: sdk.DefaultKeyDiscardInterval,
		KeyDiscardRatio:    sdk.DefaultKeyDiscardRatio,
	}

	// load config
	config.NewConfigurationService(&conf).
		LoadYamlFile(*configFile).Output()

	if err := conf.Validate(); err != nil {
		panic(err)
	}

	// setup storage
	db = badger.New(&conf)
	db.Start(context.Background())
	defer db.Stop(context.Background())

	// setup server
	s := resp.NewServer()

	s.HandleFunc("Del", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Del' command"))
		} else {
			var (
				name = args[1].Bytes()
			)
			err := db.Del(name)
			if err != nil {
				conn.WriteError(err)
			} else {
				conn.WriteInteger(1)
			}
		}
		return true
	})
	s.HandleFunc("Exists", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Exists' command"))
		} else {
			var (
				name = args[1].Bytes()
			)
			ok, err := db.Exists(name)
			if err != nil {
				conn.WriteError(err)
			} else {
				if ok {
					conn.WriteInteger(1)
				} else {
					conn.WriteInteger(0)
				}
			}
		}
		return true
	})
	s.HandleFunc("Expire", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Expire' command"))
		} else {
			var (
				name  = args[1].Bytes()
				lease = time.Duration(args[2].Integer()) * time.Second
			)
			ok, err := db.Expire(name, lease)
			if err != nil {
				conn.WriteError(err)
			} else {
				if ok {
					conn.WriteInteger(1)
				} else {
					conn.WriteInteger(0)
				}
			}
		}
		return true
	})
	s.HandleFunc("Get", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Get' command"))
		} else {
			var (
				name = args[1].Bytes()
			)
			value, err := db.Get(name)
			if err != nil {
				if errors.Is(err, sdk.ErrNil) {
					conn.WriteNull()
				} else {
					conn.WriteError(err)
				}
			} else {
				conn.WriteBytes(value)
			}
		}
		return true
	})
	s.HandleFunc("IncrBy", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) < 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'IncrBy' command"))
		} else {
			var (
				name      = args[1].Bytes()
				increment = int64(args[2].Integer())

				constraints []sdk.Constraint[int64]
			)

			for i := 2; i < len(args); i++ {
				param := strings.ToUpper(args[i].String())

				switch param {
				case "CONSTRAINT":
					// is EOF?
					if i+1 >= len(args) {
						conn.WriteError(errors.New("ERR missing constraint type"))
						return true
					}
					i++
					constraintType := strings.ToUpper(args[i].String())
					switch constraintType {
					case "<=", "LE", "LESS_OR_EQUAL":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria int64 = int64(args[i].Integer())
						constraints = append(constraints, sdk.IntegerLessOrEqual(criteria))
					case ">=", "GE", "GREATER_OR_EQUAL":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria int64 = int64(args[i].Integer())
						constraints = append(constraints, sdk.IntegerGreaterOrEqual(criteria))
					case "NON_NEGATIVE":
						constraints = append(constraints, sdk.IntegerNonNegativeValue())
					case "NON_ZERO":
						constraints = append(constraints, sdk.IntegerNonZero())
					default:
						conn.WriteError(errors.New(fmt.Sprintf("ERR unsupported constraint type '%s'", constraintType)))
						return true
					}
				}
			}

			value, err := db.IncrBy(name, increment, constraints...)
			if err != nil {
				conn.WriteError(err)
				return true
			}
			conn.WriteInteger(int(value))
		}
		return true
	})
	s.HandleFunc("IncrByFloat", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) < 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'IncrByFloat' command"))
		} else {
			var (
				name      = args[1].Bytes()
				increment = args[2].Float()

				constraints []sdk.Constraint[float64]
			)

			for i := 2; i < len(args); i++ {
				param := strings.ToUpper(args[i].String())

				switch param {
				case "CONSTRAINT":
					// is EOF?
					if i+1 >= len(args) {
						conn.WriteError(errors.New("ERR missing constraint type"))
						return true
					}
					i++
					constraintType := strings.ToUpper(args[i].String())
					switch constraintType {
					case "<", "LT", "LESS":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria = args[i].Float()
						constraints = append(constraints, sdk.NumberLess(criteria))
					case "<=", "LE", "LESS_OR_EQUAL":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria = args[i].Float()
						constraints = append(constraints, sdk.NumberLessOrEqual(criteria))
					case ">", "GT", "GREATER":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria = args[i].Float()
						constraints = append(constraints, sdk.NumberGreater(criteria))
					case ">=", "GE", "GREATER_OR_EQUAL":
						// is EOF?
						if i+1 >= len(args) {
							conn.WriteError(errors.New("ERR missing constraint criteria"))
							return true
						}
						i++
						var criteria = args[i].Float()
						constraints = append(constraints, sdk.NumberGreaterOrEqual(criteria))
					case "NON_NEGATIVE":
						constraints = append(constraints, sdk.NumberNonNegativeValue())
					case "NON_ZERO":
						constraints = append(constraints, sdk.NumberNonZero())
					default:
						conn.WriteError(errors.New(fmt.Sprintf("ERR unsupported constraint type '%s'", constraintType)))
						return true
					}
				}
			}

			value, err := db.IncrByFloat(name, increment, constraints...)
			if err != nil {
				conn.WriteError(err)
			} else {
				conn.WriteString(strconv.FormatFloat(value, 'f', 4, 64))
			}
		}
		return true
	})
	s.HandleFunc("Persist", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Persist' command"))
		} else {
			var (
				name = args[1].Bytes()
			)
			ok, err := db.Persist(name)
			if err != nil {
				conn.WriteError(err)
			} else {
				if ok {
					conn.WriteInteger(1)
				} else {
					conn.WriteInteger(0)
				}
			}
		}
		return true
	})
	s.HandleFunc("Scan", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) < 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Scan' command"))
		} else {
			var (
				cursor = args[1].Bytes()
				opts   = sdk.ScanOptions{}
			)

			for i := 2; i < len(args); i++ {
				param := strings.ToUpper(args[i].String())

				switch param {
				case "PREFIX":
					// is EOF?
					if i+1 >= len(args) {
						conn.WriteError(errors.New("ERR wrong number of arguments for 'Scan' command"))
						return true
					}
					i++
					value := args[i]
					opts.Prefix = value.Bytes()
				case "WITH_REVERSE":
					opts.Reverse = true
				case "WITH_VALUE":
					opts.PrefetchValues = true
				}
			}
			keys, err := db.Scan(cursor, opts)
			if err != nil {
				conn.WriteError(err)
			} else {
				var reply []resp.Value
				for _, key := range keys {
					value := resp.BytesValue(key)
					reply = append(reply, value)
				}
				conn.WriteArray(reply)
			}
		}
		return true
	})
	s.HandleFunc("Set", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Set' command"))
		} else {
			var (
				name  = args[1].Bytes()
				value = args[2].Bytes()
			)
			db.Set(name, value)
			conn.WriteSimpleString("OK")
		}
		return true
	})
	s.HandleFunc("Ttl", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Ttl' command"))
		} else {
			var (
				name = args[1].Bytes()
			)

			ok, ttl, err := db.Ttl(name)
			if err != nil {
				conn.WriteError(err)
			} else {
				if !ok {
					conn.WriteInteger(-2)
				} else {
					if ttl < 0 {
						conn.WriteInteger(-1)
					} else {
						conn.WriteInteger(int(ttl))
					}
				}
			}

		}
		return true
	})

	s.HandleFunc("Shutdown", func(conn *resp.Conn, args []resp.Value) bool {
		conn.WriteSimpleString("OK")
		db.Stop(context.Background())
		os.Exit(0)
		return false
	})

	fmt.Printf("server start at %s\n", conf.ListenAddress)
	if err := s.ListenAndServe(conf.ListenAddress); err != nil {
		log.Fatal(err)
	}
}
