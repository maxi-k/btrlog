{
  description = "Journal Service";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
        };

        # Read rust-toolchain.toml
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Cross-compilation setup
        pkgsCross = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
          crossSystem = {
            config = "aarch64-unknown-linux-gnu";
          };
        };

        # Common library path
        libPath = pkgs.lib.makeLibraryPath [
          # add external libraries here
        ];

        # Common build inputs
        commonBuildInputs = [
          pkgs.rustup
          pkgs.openssl
        ];

        leanstoreDeps = with pkgs; [
          cmake
          pkg-config
          llvmPackages_21.clang-tools
          cmake-language-server
          liburing
          # leanstore backends & comparisons
          capnproto
          libpq
          libmysqlconnectorcpp
          sqlite
          foundationdb
          rocksdb
          wiredtiger
          fuse
          # Additional CMake dependencies
          gtest
          gflags
          gbenchmark
          zstd
          tbb_2022_0
          fmt
        ];

        bookkeeperDeps = with pkgs; [
          jdk17
          maven
        ];

        # Cross compilation dependencies
        crossDeps = [
          pkgs.llvmPackages_21.libllvm
        ];

        # Development tools
        devTools = with pkgs; [
          terraform
          awscli2
          latexrun
          texlive.combined.scheme-full
          duckdb
          fio
          sockperf
        ];

        # R packages
        rPackages = with pkgs.rPackages; [
          dplyr tidyr purrr
          Cairo ggplot2 ggpubr ggpattern ggforce ggrepel directlabels
          lubridate slider stringr
          RColorBrewer shades
          duckdb
        ] ++ [ pkgs.R ];

        nativeBuildInputs = with pkgs; [
          git
          cargo-feature
          cargo-expand
          cargo-modules
          cargo-cross
          pkg-config
          patchelf
          (writeShellScriptBin "enable-git-hooks" "git config --local core.hooksPath .githooks/")
        ];

      in
      {
        devShells = {
          # Default development shell (native compilation)
          default = pkgs.mkShell {
            buildInputs = commonBuildInputs ++ crossDeps ++ devTools ++ rPackages ++ [ rustToolchain ] ++ leanstoreDeps ++ bookkeeperDeps;
            nativeBuildInputs = nativeBuildInputs;

            RUSTC_VERSION = "stable";
            LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages_latest.libclang.lib ];
            LD_LIBRARY_PATH = libPath;

            shellHook = ''
              export PATH=$PATH:''${CARGO_HOME:-~/.cargo}/bin
              export PATH=$PATH:''${RUSTUP_HOME:-~/.rustup}/toolchains/stable-x86_64-unknown-linux-gnu/bin/
              export LOCALE_ARCHIVE="${pkgs.glibcLocales}/lib/locale/locale-archive"

              # Configure CMake to find Nix packages
              export CMAKE_PREFIX_PATH="${pkgs.lib.concatMapStringsSep ":" (pkg: "${pkg.dev or pkg}/lib/cmake:${pkg.out or pkg}/lib/cmake") leanstoreDeps}"
              export PKG_CONFIG_PATH="${pkgs.lib.concatMapStringsSep ":" (pkg: "${pkg.dev or pkg}/lib/pkgconfig:${pkg.out or pkg}/lib/pkgconfig") leanstoreDeps}"

              echo "Development environment loaded (native compilation)"
              echo "For cross-compilation to aarch64, use: nix develop .#cross-aarch64"
              echo "CMAKE_PREFIX_PATH configured for Nix packages"
            '';

            BINDGEN_EXTRA_CLANG_ARGS =
              (builtins.map (a: ''-I"${a}/include"'') [
                pkgs.glibc.dev
              ])
              ++ [
                ''-I"${pkgs.llvmPackages_latest.libclang.lib}/lib/clang/${pkgs.llvmPackages_latest.libclang.version}/include"''
                ''-I"${pkgs.glib.dev}/include/glib-2.0"''
                ''-I${pkgs.glib.out}/lib/glib-2.0/include/''
              ];
          };

          # Cross-compilation shell for aarch64
          
          cross-aarch64 = let
            x = pkgs.pkgsCross;
            xcc = x.aarch64-multiplatform.stdenv.cc;
          in pkgs.mkShell {
            buildInputs = commonBuildInputs ++ crossDeps ++ devTools ++ rPackages ++ [ rustToolchain ];
            nativeBuildInputs = nativeBuildInputs ++ [
              # Cross-compilation toolchain
              xcc
            ];

            RUSTC_VERSION = "stable";
            LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages_latest.libclang.lib ];
            LD_LIBRARY_PATH = libPath;

            # Cargo and C/C++ compiler configuration for cross-compilation
            CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER = "${xcc}/bin/${xcc.targetPrefix}cc";
            CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-C linker=${xcc}/bin/${xcc.targetPrefix}cc";

            # Set CC and CXX for build scripts (like aws-lc-sys)
            CC_aarch64_unknown_linux_gnu = "${xcc}/bin/${xcc.targetPrefix}cc";
            CXX_aarch64_unknown_linux_gnu = "${xcc}/bin/${xcc.targetPrefix}c++";
            AR_aarch64_unknown_linux_gnu = "${xcc}/bin/${xcc.targetPrefix}ar";

            # Target-specific configuration
            CFLAGS_aarch64_unknown_linux_gnu = "-O2";
            CXXFLAGS_aarch64_unknown_linux_gnu = "-O2";

            shellHook = ''
              export PATH=$PATH:''${CARGO_HOME:-~/.cargo}/bin
              export PATH=$PATH:''${RUSTUP_HOME:-~/.rustup}/toolchains/stable-x86_64-unknown-linux-gnu/bin/
              export LOCALE_ARCHIVE="${pkgs.glibcLocales}/lib/locale/locale-archive"

              # Set up cargo config for cross-compilation
              mkdir -p .cargo
              cat > .cargo/config.toml <<EOF
              [target.aarch64-unknown-linux-gnu]
              linker = "${xcc}/bin/${xcc.targetPrefix}cc"
              EOF

              echo "Cross-compilation environment loaded for aarch64-unknown-linux-gnu"
              echo "Linker: ${xcc}/bin/${xcc.targetPrefix}cc"
              echo "You can now run: cargo build --target aarch64-unknown-linux-gnu"
            '';

            BINDGEN_EXTRA_CLANG_ARGS =
              (builtins.map (a: ''-I"${a}/include"'') [
                pkgs.glibc.dev
              ])
              ++ [
                ''-I"${pkgs.llvmPackages_latest.libclang.lib}/lib/clang/${pkgs.llvmPackages_latest.libclang.version}/include"''
                ''-I"${pkgs.glib.dev}/include/glib-2.0"''
                ''-I${pkgs.glib.out}/lib/glib-2.0/include/''
              ];
          };
        };

        # Legacy support for `nix-shell` users
        devShell = self.devShells.${system}.default;
      }
    );
}
