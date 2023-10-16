package sdk

import (
	"fmt"
	"log"
	"time"
)

type Config struct {
	ListenAddress      string        `yaml:"ListenAddress"`
	Engine             string        `yaml:"Engine"`
	DataPath           string        `yaml:"DataPath"`
	KeyDiscardInterval time.Duration `yaml:"KeyDiscardInterval"`
	KeyDiscardRatio    float64       `yaml:"KeyDiscardRatio"`
	LogFlagsToken      []string      `yaml:"LogFlags"`
}

func (conf *Config) LogFlags() (int, error) {
	if len(conf.LogFlagsToken) == 0 {
		return DefaultLogFlags, nil
	}

	var (
		tokens []string = conf.LogFlagsToken
		value  int      = 0
		err    error
	)

	if (len(tokens) == 1) && (tokens[0] == LOG_FLAG_TOKEN_NONE) {
		return value, nil
	}

	for _, token := range tokens {
		switch token {
		case LOG_FLAG_TOKEN_DATE:
			value |= log.Ldate
		case LOG_FLAG_TOKEN_TIME:
			value |= log.Ltime
		case LOG_FLAG_TOKEN_UTC:
			value |= log.LUTC
		case LOG_FLAG_TOKEN_MSGPREFIX:
			value |= log.Lmsgprefix
		case LOG_FLAG_TOKEN_DEFAULT:
			value |= DefaultLogFlags
		case LOG_FLAG_TOKEN_NONE:
			err = fmt.Errorf("LogFlag '%s' cannot mix with other flag values", token)
		default:
			if len(token) > 0 {
				// unsupported LogFlag
				if err == nil {
					err = fmt.Errorf("unsupported LogFlag '%s'", token)
				}
			}
		}
	}

	return value, err
}
