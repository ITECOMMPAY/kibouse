package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"kibouse/db"
)

type modelNames struct {
	SourceName string
	ChName     string
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func executeTemplateFromFile(model modelNames, file string, writer io.Writer) error {
	tmpl, err := template.ParseFiles(file)
	if err != nil {
		return errors.New("cannot parse " + file)
	}

	return tmpl.Execute(writer, model)
}

// RootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "autogen",
	Short: "generate source code for kibouse models",
	Long:  "autogen tool uses for generating new blank kibouse models and wrappers for it",
	Run: func(cmd *cobra.Command, args []string) {
		params := []struct {
			template string
			output   string
		}{
			{
				template: "tpl/model.tmpl",
				output:   dataFolder + "/models/" + strings.ToLower(sourceCodeName) + ".go",
			},
			{
				template: "tpl/wrapper.tmpl",
				output:   dataFolder + "/wrappers/" + strings.ToLower(sourceCodeName) + ".go",
			},
		}

		for _, param := range params {
			outputFile, err := os.Create(param.output)

			check(err)
			check(
				executeTemplateFromFile(
					modelNames{
						SourceName: sourceCodeName,
						ChName:     db.DataStorageTablesPrefix + chTableName,
					},
					param.template,
					outputFile,
				),
			)
			outputFile.Close()
		}
	},
}

var dataFolder string
var chTableName string
var sourceCodeName string

func init() {
	rootCmd.Flags().StringVarP(
		&dataFolder,
		"data_folder",
		"d",
		"../data",
		"kibouse data folder for writing generated output",
	)
	rootCmd.Flags().StringVarP(
		&chTableName,
		"clickhouse_table",
		"c",
		"",
		"clickhouse table for storing data",
	)
	rootCmd.Flags().StringVarP(
		&sourceCodeName,
		"source_name",
		"s",
		"",
		"source code name for model",
	)

	rootCmd.MarkFlagRequired("clickhouse_table")
	rootCmd.MarkFlagRequired("source_name")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
