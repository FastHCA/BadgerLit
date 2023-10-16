package main

import (
	"badgerlit/sdk"
	"badgerlit/storage/badger"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
)

func main() {
	var (
		addr string = ":16379"
		db   sdk.Storage
	)

	config := sdk.Config{
		Engine:             "file",
		DataPath:           "./.data/dump",
		KeyDiscardInterval: 10 * time.Second,
		KeyDiscardRatio:    0.7,
		LogFlagTokens:      strings.Split("default,msgprefix", ","),
	}
	db = badger.New(&config)
	db.Start(context.Background())
	defer db.Stop(context.Background())

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
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'IncrBy' command"))
		} else {
			var (
				name  = args[1].Bytes()
				value = int64(args[2].Integer())
			)
			value, err := db.IncrBy(name, value)
			if err != nil {
				conn.WriteError(err)
				return true
			}
			conn.WriteInteger(int(value))
		}
		return true
	})
	s.HandleFunc("IncrByFloat", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'IncrByFloat' command"))
		} else {
			var (
				name  = args[1].Bytes()
				value = args[2].Float()
			)
			value, err := db.IncrByFloat(name, value)
			if err != nil {
				conn.WriteError(err)
			} else {
				conn.WriteString(strconv.FormatFloat(value, 'f', 4, 64))
			}
		}
		return true
	})
	s.HandleFunc("Scan", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) < 2 || len(args)%2 != 0 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'Scan' command"))
		} else {
			var (
				cursor = args[1].Bytes()
				opts   = sdk.ScanOptions{}
			)

			for i := 2; i < len(args); i += 2 {
				param := strings.ToUpper(args[i].String())
				value := args[i+1]

				switch param {
				case "PREFIX":
					opts.Prefix = value.Bytes()
				case "REVERSE":
					opts.Reverse = value.Bool()
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
				return true
			}

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
		return true
	})

	fmt.Printf("server start at %s\n", addr)
	if err := s.ListenAndServe(addr); err != nil {
		log.Fatal(err)
	}
}
