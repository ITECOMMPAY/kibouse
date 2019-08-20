package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	"kibouse/app"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "kibouse",
	Short: "Proxy kibana requests to clickhouse and vise versa",
	Long:  "Proxy kibana requests to clickhouse and vise versa",
	Run: func(cmd *cobra.Command, args []string) {
		adapter, err := app.New(cfgFile)
		if err != nil {
			log.Fatal(fmt.Sprintf("%+v", err))
		}

		log.Fatal(adapter.Run())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVar(
		&cfgFile,
		"config",
		"../config/config.yaml",
		"config file (default is ../config/config.yaml)",
	)
}
