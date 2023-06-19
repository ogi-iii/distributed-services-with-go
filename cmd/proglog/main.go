package main

import (
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/ogi-iii/proglog/internal/agent"
	"github.com/ogi-iii/proglog/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	cli := &cli{}
	cmd := &cobra.Command{
		Use:     "proglog",
		PreRunE: cli.setupConfig, // will be called before RunE()
		RunE:    cli.run,
	}
	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Execute(); err != nil { // call the functions: PreRunE() -> RunE()
		log.Fatal(err)
	}
}

type cli struct { // contains logic and data that is common to all commands
	cfg cfg
}

type cfg struct { // cannot parse without error handling
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func setupFlags(cmd *cobra.Command) error { // setup the flags for configuration
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	cmd.Flags().String("config-file", "", "Path to the configuration file.")

	dataDir := path.Join(os.TempDir(), "proglog")
	cmd.Flags().String("data-dir", dataDir, "Directory to store log and raft data.") // set the default value

	cmd.Flags().String("node-name", hostname, "Unique server ID.")

	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")

	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")

	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")

	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	cmd.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority.")

	return viper.BindPFlags(cmd.Flags())
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	// setup by configuration file
	viper.SetConfigFile(configFile)
	if err = viper.ReadInConfig(); err != nil {
		// it's ok if the configuration file doesn't exist
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}
	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")

	c.cfg.ACLModelFile = viper.GetString("acl-model-file")
	c.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")

	c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")

	c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

	if c.cfg.ServerTLSConfig.CertFile != "" && c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.ServerTLSConfig.Server = true
		c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(c.cfg.ServerTLSConfig)
		if err != nil {
			return err
		}
	}
	if c.cfg.PeerTLSConfig.CertFile != "" && c.cfg.PeerTLSConfig.KeyFile != "" {
		c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(c.cfg.PeerTLSConfig)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	var err error
	agent, err := agent.New(c.cfg.Config) // start our app with the configuration which was setup in PreRunE()
	if err != nil {
		return err
	}
	sigc := make(chan os.Signal, 1) // the channel to receive the os signal
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)

	<-sigc // wait until receiving the os signal to terminate our app

	return agent.Shutdown() // shutdown our app gracefully
}
