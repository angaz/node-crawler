{
  description = "Ethereum network crawler, API, and frontend";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    devshell = {
      url = "github:numtide/devshell";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    templ.url = "github:a-h/templ?ref=v0.3.960";
  };

  outputs = inputs@{
    self,
    nixpkgs,
    devshell,
    flake-parts,
    gitignore,
    ...
  }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        devshell.flakeModule
        flake-parts.flakeModules.easyOverlay
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem = { config, pkgs, system, ... }: let
        inherit (gitignore.lib) gitignoreSource;
        templ = inputs.templ.packages.${system}.templ;
      in {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              orioledb = final.buildPostgresqlExtension rec {
                pname = "orioledb";
                version = "beta8";

                src = prev.fetchFromGitHub {
                  owner = "orioledb";
                  repo = "orioledb";
                  rev = version;
                  sha256 = "";
                };

                makeFlags = [ "USE_PGXS=1" ];
              };
            })
          ];
        };

        # Attrs for easyOverlay
        overlayAttrs = {
          inherit (config.packages)
            nodeCrawler
            ;
        };

        packages = {
          nodeCrawler = pkgs.buildGo124Module rec {
            pname = "crawler";
            version = "0.0.0";

            src = gitignoreSource ./.;
            subPackages = [ "cmd/crawler" ];

            vendorHash = "sha256-EY1kSsjuUbUpSaX3QktMKLoHh2T2HQUOuUe6V2MZE2g=";

            doCheck = false;

            preBuild = ''
              ${templ}/bin/templ generate
            '';

            ldflags = [
              "-s"
              "-w"
              "-X github.com/ethereum/node-crawler/pkg/version.version=${version}"
              "-X github.com/ethereum/node-crawler/pkg/version.gitCommit=${self.rev or self.dirtyRev}"
            ];
          };
        };

        devshells.default = {
          commands = [
            {
              name = "go-mod-vendor-hash";
              help = "Gets the vendor hash for the go modules";
              command = ''
                nix-prefetch --option extra-experimental-features flakes --silent \
                  '{ sha256 }: (builtins.getFlake (toString ./.)).packages.x86_64-linux.nodeCrawler.goModules.overrideAttrs (_: { vendorSha256 = sha256; })'
              '';
            }
            {
              name = "go-mod-upgrade";
              help = "Upgrades the go dependencies. Prints the new vendorHash.";
              command = ''
                go get -u ./... && \
                go mod tidy && \
                go-mod-vendor-hash
              '';
            }
          ];

          packages = with pkgs; [
            go_1_24
            golangci-lint
            graphviz
            nix-prefetch
            nodejs
            postgresql_17
            sqlite-interactive
            templ
            # orioledb
          ];
        };
      };

      flake = rec {
        nixosModules.default = nixosModules.nodeCrawler;
        nixosModules.nodeCrawler = { config, lib, pkgs, ... }:
        with lib;
        let
          cfg = config.services.nodeCrawler;
          apiAddress = "${cfg.api.address}:${toString cfg.api.port}";
        in
        {
          options.services.nodeCrawler = {
            enable = mkEnableOption (self.flake.description);

            hostName = mkOption {
              type = types.str;
              default = "localhost";
              description = "Hostname to serve Node Crawler on.";
            };

            nginx = mkOption {
              type = types.attrs;
              default = { };
              example = literalExpression ''
                {
                  forceSSL = true;
                  enableACME = true;
                }
              '';
              description = "Extra configuration for the vhost. Useful for adding SSL settings.";
            };

            stateDir = mkOption {
              type = types.path;
              default = /var/lib/node_crawler;
              description = "Directory where the databases will exist.";
            };

            crawlerDatabaseName = mkOption {
              type = types.str;
              default = "";
              description = "Name of the file within the `stateDir` for storing the data for the crawler.";
            };

            statsDatabaseName = mkOption {
              type = types.str;
              default = "";
              description = "Name of the file within the `stateDir` for storing the stats for the crawler.";
            };

            snapshotDirname = mkOption {
              type = types.str;
              default = "/var/lib/postgres_backups/nodecrawler";
              description = "Snapshots directory name within the `stateDir`";
            };

            user = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = "User account under which Node Crawler runs.";
            };

            group = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = "Group account under which Node Crawler runs.";
            };

            dynamicUser = mkOption {
              type = types.bool;
              default = true;
              description = ''
                Runs the Node Crawler as a SystemD DynamicUser.
                It means SystenD will allocate the user at runtime, and enables
                some other security features.
                If you are not sure what this means, it's safe to leave it default.
              '';
            };

            dailyBackup = mkOption {
              type = types.bool;
              default = true;
              description = ''
                Takes a daily backup of the Postgres database, saving it to the `snapshotDirname`.
              '';
            };

            dailyBackupRetention = mkOption {
              type = types.int;
              default = 7;
              description = "Number of days to keep backups for.";
            };

            api = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = "Enables the Node Crawler API server.";
              };

              pprof = mkOption {
                type = types.bool;
                default = false;
                description = "Enable the pprof http server";
              };

              address = mkOption {
                type = types.str;
                default = "127.0.0.1";
                description = "Listen address for the API server.";
              };

              port = mkOption {
                type = types.port;
                default = 10000;
                description = "Listen port for the API server.";
              };

              metricsAddress = mkOption {
                type = types.str;
                default = "0.0.0.0:9190";
                description = "Address on which the metrics server listens. This is NOT added to the firewall.";
              };

              enodeAddr = mkOption {
                type = types.str;
                default = "";
                description = "Address to use for displaying the enodes on the help page";
                example = "127.0.0.1";
              };

              maxPoolConns = mkOption {
                type = types.int;
                default = 16;
                description = "Max number of open connections to the database.";
              };
            };

            crawler = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = "Enables the Node Crawler API server.";
              };

              pprof = mkOption {
                type = types.bool;
                default = false;
                description = "Enable the pprof http server";
              };

              geoipdb = mkOption {
                type = types.path;
                default = config.services.geoipupdate.settings.DatabaseDirectory + "/GeoLite2-City.mmdb";
                description = ''
                  Location of the GeoIP database.

                  If the default is used, the `geoipupdate` service files.
                  So you will need to configure it.
                  Make sure to enable the `GeoLite2-City` edition.

                  If you do not want to enable the `geoipupdate` service, then
                  the `GeoLite2-City` file needs to be provided.
                '';
              };

              openFirewall = mkOption {
                type = types.bool;
                default = true;
                description = "Opens the crawler ports.";
              };

              executionWorkers = mkOption {
                type = types.int;
                default = 16;
                description = "Number of crawler workers to start.";
              };

              discWorkers = mkOption {
                type = types.int;
                default = 2;
                description = "Number of discovery crawler workers to start.";
              };

              maxPoolConns = mkOption {
                type = types.int;
                default = (cfg.crawler.executionWorkers) + (cfg.crawler.discWorkers) + (cfg.crawler.listenerCount * 8);
                description = "Max number of open connections to the database.";
              };

              metricsAddress = mkOption {
                type = types.str;
                default = "0.0.0.0:9191";
                description = "Address on which the metrics server listens. This is NOT added to the firewall.";
              };

              nextCrawlSuccess = mkOption {
                type = types.str;
                default = "12h";
                description = "Next crawl value if the crawl was successful.";
              };

              nextCrawlFail = mkOption {
                type = types.str;
                default = "48h";
                description = "Next crawl value if the crawl was unsuccessful.";
              };

              nextCrawlNotEth = mkOption {
                type = types.str;
                default = "336h"; # 14d
                description = "Next crawl value if the node was not an eth node.";
              };

              listenPortStart = mkOption {
                type = types.port;
                default = 30303;
                description = "Port number to start on for the list of listeners.";
              };

              listenerCount = mkOption {
                type = types.int;
                default = 16;
                description = "Number of listeners.";
              };

              githubTokenFile = mkOption {
                type = types.str;
                default = "./github_token";
                description = "File path to the GitHub token file. Set to an empty string if you don't want to use one.";
              };
            };

            postgresql = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = "Enables the Postgres database.";
              };
            };
          };

          config = mkIf cfg.enable {
            networking.firewall = mkIf cfg.crawler.openFirewall (
              let
                portsFn = l: i:
                  if i == cfg.crawler.listenerCount then l
                  else portsFn (l ++ [(cfg.crawler.listenPortStart + i)]) (i + 1);
                ports = portsFn [] 0;
              in
              {
                allowedUDPPorts = ports;
                allowedTCPPorts = ports;
              }
            );

            systemd.services = {
              node-crawler-crawler = mkIf cfg.crawler.enable {
                description = "Node Cralwer, the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                requires = [ "postgresql.service" ];
                after = [ "network.target" ];

                serviceConfig = {
                  ExecStart =
                  let
                    args = [
                      "--crawler-db=${cfg.crawlerDatabaseName}"
                      "--geoipdb=${cfg.crawler.geoipdb}"
                      "--github-token=${cfg.crawler.githubTokenFile}"
                      "--listen-start-port=${toString cfg.crawler.listenPortStart}"
                      "--metrics-addr=${cfg.crawler.metricsAddress}"
                      "--next-crawl-fail=${cfg.crawler.nextCrawlFail}"
                      "--next-crawl-not-eth=${cfg.crawler.nextCrawlNotEth}"
                      "--next-crawl-success=${cfg.crawler.nextCrawlSuccess}"
                      "--postgres=\"host=/var/run/postgresql user=nodecrawler database=nodecrawler pool_max_conns=${toString cfg.crawler.maxPoolConns}\""
                      "--stats-db=${cfg.statsDatabaseName}"
                      "--execution-workers=${toString cfg.crawler.executionWorkers}"
                      "--disc-workers=${toString cfg.crawler.discWorkers}"
                    ];
                  in
                  "${pkgs.nodeCrawler}/bin/crawler --pprof=${if cfg.crawler.pprof then "true" else "false"} crawl ${concatStringsSep " " args}";

                  WorkingDirectory = cfg.stateDir;
                  StateDirectory = optional (cfg.stateDir == /var/lib/node_crawler) "node_crawler";

                  DynamicUser = cfg.dynamicUser;
                  Group = cfg.group;
                  User = cfg.user;

                  Restart = "on-failure";
                };
              };
              node-crawler-api = mkIf cfg.api.enable {
                description = "Node Cralwer API, the API for the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                requires = [ "postgresql.service" ];
                after = [ "network.target" ]
                  ++ optional cfg.crawler.enable "node-crawler-crawler.service";

                serviceConfig = {
                  ExecStart =
                  let
                    args = [
                      "--api-addr=${apiAddress}"
                      "--enode-addr=${cfg.api.enodeAddr}"
                      "--metrics-addr=${cfg.api.metricsAddress}"
                      "--snapshot-dir=${cfg.snapshotDirname}"
                      "--postgres=\"host=/var/run/postgresql user=nodecrawler database=nodecrawler pool_max_conns=${toString cfg.api.maxPoolConns}\""
                    ];
                  in
                  "${pkgs.nodeCrawler}/bin/crawler --pprof=${if cfg.api.pprof then "true" else "false"} api ${concatStringsSep " " args}";

                  WorkingDirectory = cfg.stateDir;
                  StateDirectory = optional (cfg.stateDir == /var/lib/node_crawler) "node_crawler";

                  DynamicUser = cfg.dynamicUser;
                  Group = cfg.group;
                  User = cfg.user;

                  Restart = "on-failure";
                };
              };

              node-crawler-daily-backup = mkIf cfg.dailyBackup {
                enable = true;
                description = ''Daily Postgres backup for the Node Crawler service.'';
                requires = [ "postgresql.service" ];
                startAt = "*-*-* 00:00:00";

                serviceConfig = {
                  Type = "oneshot";
                  Group = "postgres";
                  User = "postgres";
                  StateDirectory = "postgres_backups";
                };

                path = [
                  pkgs.coreutils
                  config.services.postgresql.package
                ];

                script = ''
                  set -e -o pipefail

                  mkdir -p "${cfg.snapshotDirname}"

                  find "${cfg.snapshotDirname}" -mtime +${toString cfg.dailyBackupRetention} -name '*.pgdump' -delete

                  dump_name="${cfg.snapshotDirname}/nodecrawler_$(date --utc +%Y%m%dT%H%M%S).pgdump"

                  pg_dump \
                    --format custom \
                    --file "''${dump_name}.part" \
                    --host /var/run/postgresql \
                    nodecrawler

                  mv "''${dump_name}.part" "''${dump_name}"
                '';
              };
            };

            environment.systemPackages = with pkgs; [
              timescaledb-tune
            ];

            services = {
              nginx = {
                enable = true;
                upstreams.nodeCrawlerApi.servers."${apiAddress}" = { };
                virtualHosts."${cfg.hostName}" = mkMerge [
                  cfg.nginx
                  {
                    locations = {
                      "/" = {
                        proxyPass = "http://nodeCrawlerApi/";
                      };
                    };
                  }
                ];
              };
              postgresql = mkIf cfg.postgresql.enable {
                enable = true;
                enableJIT = false;
                package = pkgs.postgresql_17;
                extensions = psql: with psql; [
                  pg_repack
                  timescaledb
                ];
                settings = {
                  max_connections = (cfg.crawler.maxPoolConns + cfg.api.maxPoolConns) * 1.15;
                  shared_preload_libraries = concatStringsSep "," [
                    "pg_repack"
                    "pg_stat_statements"
                    "timescaledb"
                  ];

                  # Performance tuning.
                  checkpoint_timeout = "30min";
                  effective_io_concurrency = 200;
                  max_wal_size = "16GB";
                  shared_buffers = "256MB";
                  wal_buffers = "16MB";
                  wal_writer_delay = "4s";
                };
                ensureDatabases = [ "nodecrawler" ];
                ensureUsers = [
                  {
                    name = "nodecrawler";
                    ensureDBOwnership = true;
                    ensureClauses = {
                      login = true;
                    };
                  }
                ];
              };
            };
          };
        };
      };
  };
}
