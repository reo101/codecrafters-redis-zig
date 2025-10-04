{
  inputs = {
    nixpkgs = {
      url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    };
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    systems = {
      url = "github:nix-systems/default";
    };
  };

  outputs = inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; } ({ withSystem, flake-parts-lib, ... }: {
    systems = import inputs.systems;

    debug = true;

    perSystem = { lib, pkgs, system, ... }: {
      devShells.default = pkgs.mkShell {
        buildInputs = [
          ### Main
          pkgs.zig
          pkgs.zls

          ### Testing
          pkgs.netcat
          pkgs.codecrafters-cli
        ];
      };
    };
  });
}
