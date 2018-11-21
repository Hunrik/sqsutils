package main

import (
	"errors"
	"log"
	"os"
	"regexp"
	"sort"

	"github.com/Hunrik/sqsutils/internal/load"
	"github.com/Hunrik/sqsutils/internal/save"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "SQS Toolbox"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "queue, q",
			Value: "",
			Usage: "Queue to operate on",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "load",
			Aliases: []string{"l"},
			Usage:   "Loads data to the specified queue",
			Action: func(c *cli.Context) error {
				args := c.Args()
				file := args.First()
				if file == "" {
					return errors.New("Missing file parameter")
				}
				queue := args.Get(1)
				if queue == "" {
					return errors.New("Missing queue parameter")
				}

				formatString := c.String("format")
				return load.Load(file, queue, formatString)
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "format, f",
					Value: "%s",
				},
			},
		},
		{
			Name:    "save",
			Aliases: []string{"s"},
			Usage:   "Saves documents from queue to file",
			Action: func(c *cli.Context) error {
				args := c.Args()
				queue := args.First()
				if queue == "" {
					return errors.New("Missing queue parameter")
				}
				file := args.Get(1)
				if file == "" {
					return errors.New("Missing file parameter")
				}
				regex := &regexp.Regexp{}
				regexPattern := c.String("regex")
				if regexPattern != "" {
					regex = regexp.MustCompile(regexPattern)
				}
				return save.Save(queue, file, regex)
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "regex, r",
					Value: "",
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
