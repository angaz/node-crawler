{
  description = "Ethereum network crawler, API, and frontend";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    devshell.url = "github:numtide/devshell";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, devshell, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ devshell.overlays.default ];
        };
      in rec {
        packages = {
          nodeCrawler = pkgs.buildGoModule rec {
            pname = "crawler";
            version = "0.0.0";

            src = ./.;
            subPackages = [ "cmd/crawler" ];

            vendorHash = "sha256-nR6YsXZvIUupDHGCgOYELDpJVbbPc1SPK9LdwnL5sAQ=";

            doCheck = false;

            CGO_ENABLED = 0;

            ldflags = [
              "-s"
              "-w"
              "-extldflags -static"
            ];
          };
          nodeCrawlerFrontend = pkgs.buildNpmPackage rec {
            pname = "frontend";
            version = "0.0.0";

            src = ./frontend;

            npmDepsHash = "sha256-1nLQVoNkiA4x97UcPe8rNMXa7bYCskazpJesWVLnDHk=";

            installPhase = ''
              mkdir -p $out/share
              cp -r build/ $out/share/frontend
            '';
          };
        };
        nixosModules.default = nixosModules.nodeCrawler;
        nixosModules.nodeCrawler = { config, lib, pkgs }:
        with lib;
        let
          cfg = config.services.nodeCrawler;
          apiAddress = "${cfg.api.address}:${toString cfg.api.port}";
          crawlerDBPath = "${cfg.stateDir}/${cfg.crawlerDatabaseName}";
          apiDBPath = "${cfg.stateDir}/${cfg.apiDatabaseName}";
        in
        {
          options.services.nodeCrawler = {
            enable = mkEnableOption (mdDoc self.flake.description);

            hostName = mkOption {
              type = types.str;
              default = "localhost";
              description = mdDoc "Hostname to serve Node Crawler on.";
            };

            stateDir = mkOption {
              type = types.path;
              default = "/var/lib/node_crawler";
              description = mdDoc "The directory containing the Node Crawler databases.";
            };

            crawlerDatabaseName = mkOption {
              type = types.str;
              default = "crawler.db";
              description = mkDoc "Name of the file within the `stateDir` for storing the data for the crawler.";
            };

            apiDatabaseName = mkOption {
              type = types.str;
              default = "api.db";
              description = mkDoc "Name of the file within the `stateDir` for storing the data for the API.";
            };

            user = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = mdDoc "User account under which Node Crawler runs.";
            };

            group = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = mdDoc "Group account under which Node Crawler runs.";
            };

            api = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = mkDoc "Enables the Node Crawler API server.";
              };

              address = mkOption {
                type = types.str;
                default = "127.0.0.1";
                description = mkDoc "Listen address for the API server.";
              };

              port = mkOption {
                type = types.ints.unsigned;
                default = 10000;
                description = mkDoc "Listen port for the API server.";
              };
            };

            crawler = {
              geoipdb = mkOption {
                type = types.path;
                default = config.service.geoipupdate.settings.DatabaseDirectory + "/GeoLite2-Country.mmdb";
                description = mkDoc ''
                  Location of the GeoIP database.

                  If the default is used, the `geoipupdate` service files.
                  So you will need to configure it.
                  Make sure to enable the `GeoLite2-Country` edition.

                  If you do not want to enable the `geoipupdate` service, then
                  the `GeoLite2-Country` file needs to be provided.
                '';
              };

              network = {
                type = types.str;
                default = "mainnet";
                example = "goerli";
                description = "Name of the network to crawl. Defaults to Mainnet.";
              };
            };
          };

          config = mkIf cfg.enable {
            users.users = optionalAttrs (cfg.user == "nodecrawler") {
              nodecrawler = {
                group = cfg.group;
                uid = config.ids.uids.nodecrawler;
              };
            };

            users.groups = optionalAttrs (cfg.group == "nodecrawler") {
              nodecrawler.gid = config.ids.gids.nodecrawler;
            };

            systemd.services = {
              node-crawler-crawler = {
                description = "Node Cralwer, the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                after = [ "network.target" ];

                script =
                let
                  args = [
                    "--crawler-db=${crawlerDBPath}"
                    "--geoipdb=${cfg.crawler.geoipdb}"
                  ]
                  ++ optional (cfg.crawler.network == "goerli") "--goerli"
                  ++ optional (cfg.crawler.network == "sepolia") "--sepolia";
                in
                ''
                  ${pkgs.nodeCrawler}/bin/crawler crawl ${concatstringsSep " " args}
                '';

                serviceConfig = {
                  WorkingDirectory = cfg.stateDir;
                  Group = cfg.group;
                  User = cfg.user;
                };
              };
              node-crawler-api = {
                description = "Node Cralwer API, the API for the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                after = [ "network.target" ]
                  ++ optional cfg.crawler.enable "node-crawler-crawler.service";

                script =
                let
                  args = [
                    "--addr=${apiAddress}"
                    "--crawler-db=${crawlerDBPath}"
                    "--api-db=${apiDBPath}"
                  ];
                in
                ''
                  ${pkgs.nodeCrawler}/bin/crawler api ${concatstringsSep " " args}
                '';

                serviceConfig = {
                  WorkingDirectory = cfg.stateDir;
                  Group = cfg.group;
                  User = cfg.user;
                };
              };
            };

            services.nginx = {
              enable = true;
              upstreams.nodeCrawlerApi.servers."${apiAddress}" = { };
              virtualHosts."${cfg.hostName}" = {
                root = mkForce "${pkgs.nodeCrawlerFrontend}/share/frontend";
                locations = {
                  "/" = {
                    index = "index.html";
                    tryFiles = "$uri $uri/ /index.html";
                  };
                  "/v1/" = {
                    proxyPass = "http://nodeCrawlerApi/v1/";
                  };
                };
              };
            };
          };
        };
        devShell = pkgs.devshell.mkShell {
          packages = with pkgs; [
            go
            golangci-lint
            nodejs
            sqlite
          ];
        };
      });
}

