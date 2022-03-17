#!/bin/bash

# Script that downloads the sgr client, adds a default engine and (optionally)
# registers the user on Splitgraph Cloud.
#
# Works on Linux/OSX:
#
# bash -c "$(curl -sL https://github.com/splitgraph/splitgraph/releases/latest/download/install.sh)"
#
# For other installation methods, including Windows/pip/docker-compose, see
# https://www.splitgraph.com/docs/installation/.

set -eo pipefail

SGR_VERSION=${SGR_VERSION-0.3.7}
INSTALL_DIR=${INSTALL_DIR-$HOME/.splitgraph}
# Set IGNORE_SGR_EXISTS to keep going if sgr already exists.
# Set SKIP_BINARY=1 to skip downloading sgr
# Set SKIP_ENGINE=1 to skip setting up the engine.
BINARY=
ENGINE_PORT=${ENGINE_PORT-6432}

CLOUD_SKIPPED=

if [ -x /usr/bin/tput ] && tput setaf 1 >&/dev/null; then
  bold=$(tput bold)
  blue=$(tput setaf 4)
  bblue=$(tput bold)${blue}
  end=$(tput sgr0)
  cyan=$(tput setaf 6)
  red=$(tput setaf 1)
  green=$(tput setaf 2)
fi

_die() {
  echo "${red}Fatal:${end} $@"
  exit 1
}

_check_sgr_exists() {
  set +e
  current_sgr=$(sgr --version 2>/dev/null)
  ret=$?
  set -e
  if [ $ret == 0 ]; then
    if [ -n "$IGNORE_SGR_EXISTS" ]; then
      echo "$current_sgr already exists on this machine. Continuing anyway."
    else
      _die "$current_sgr already exists on this machine. You can upgrade it with sgr upgrade or rerun this with IGNORE_SGR_EXISTS=1 to ignore this message"
    fi
  fi
}

_get_binary_name() {
  os=$(uname)
  architecture=$(uname -m)

  if [ "$architecture" != x86_64 ]; then
    _die "Single binary method method only supported on x64 architectures. Please see https://www.splitgraph.com/docs/installation/ for other installation methods."
  fi

  if [ "$os" == Linux ]; then
    BINARY="sgr-linux-x86_64"
  elif [ "$os" == Darwin ]; then
    BINARY="sgr-osx-x86_64"
  else
    _die "This installation method only supported on Linux/OSX. Please see https://www.splitgraph.com/docs/installation/ for other installation methods."
  fi
}

_install_binary () {
  if [ -n "$SKIP_BINARY" ]; then
    echo "Skipping sgr installation as \$SKIP_BINARY is set."
    return
  fi

  _check_sgr_exists

  URL="https://github.com/splitgraph/splitgraph/releases/download/v${SGR_VERSION}"/$BINARY
  # on OS X, splitgraph.spec is called with --onedir to output .tgz of exe and shlibs
  if [ "$BINARY" == "sgr-osx-x86_64.tgz" ] ; then
    echo "Installing the compressed sgr binary and deps from $URL into $INSTALL_DIR"
    echo "Installing sgr binary and deps into $INSTALL_DIR/pkg"
  fi

}

_setup_engine() {
  if [ -n "$SKIP_ENGINE" ]; then
    echo "Skipping engine setup as \$SKIP_ENGINE is set."
    return
  fi

  if ! docker info > /dev/null 2>&1; then
    _die "Docker doesn't appear to be running on this machine. Adding Splitgraph to existing PostgreSQL installations is currently unsupported."
  fi

  echo "Setting up a Splitgraph engine."
  echo "The engine will be set up with default credentials:"
  echo "${blue}"
  echo "username: sgr"
  echo "password: password"
  echo "port: ${ENGINE_PORT}"
  echo "${end}"
  echo "These credentials are just for this local Postgres installation"
  echo "and can be changed after install."
  echo
  "$INSTALL_DIR/sgr" engine add --password password --port "${ENGINE_PORT}" || (
    _die "Error setting up the engine. If an engine already exists, you can skip this step by setting \$SKIP_ENGINE=1. In case of a port conflict, you can restart the installer with ENGINE_PORT set to some other port."
  )

  "$INSTALL_DIR/sgr" engine version
  "$INSTALL_DIR/sgr" status
  echo "Engine successfully set up."
  echo
}

_register_cloud() {
  echo
  echo "You can now register on data.splitgraph.com (the registry)."
  echo "This lets you push/pull datasets and use public data to build"
  echo "your own datasets using Splitfiles."
  echo
  echo "If you do not wish to register now, you can do it later via"
  echo "${green}sgr cloud register${end} or log in through ${green}sgr cloud login${end} / GitHub OAuth."
  echo
  read -p "Would you like to register now [y/N]? " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    "$INSTALL_DIR/sgr" cloud register
  else
    CLOUD_SKIPPED=1
  fi

}


_welcome() {
  echo
  echo "${green}${bold}Installation complete!${end}"
  echo
  echo "sgr $SGR_VERSION has been installed to ${blue}$INSTALL_DIR/sgr${end}."
  echo "Your configuration file is located at ${blue}$INSTALL_DIR/.sgconfig${end}."
  echo "Make sure to add sgr to your \$PATH, for example by doing:"
  echo
  echo "    ${blue}sudo ln -s $INSTALL_DIR/sgr /usr/local/bin/sgr${end}"
  echo
  echo
  echo "Next steps: "
  echo
  echo "  Check out the example projects"
  echo "    ${bblue}https://github.com/splitgraph/splitgraph/tree/v$SGR_VERSION/examples${end}"
  echo
  echo "  Try out the quickstart guide"
  echo "    ${bblue}https://www.splitgraph.com/docs/getting-started/five_minute_demo${end}"
  echo
  echo "  Clone a dataset"
  echo "    ${bblue}sgr clone splitgraph/domestic_us_flights${end}"
  echo
  echo "  Or inspect it without cloning it"
  echo "    ${bblue}sgr table -r data.splitgraph.com splitgraph/domestic_us_flights:latest flights${end}"
  echo
  echo "  Build your own dataset using a Splitfile"
  echo "    ${bblue}curl -sSL https://raw.githubusercontent.com/splitgraph/splitgraph/v$SGR_VERSION/examples/us-election/qoz_vote_fraction.splitfile \\"
  echo "     | sgr build - -o qoz_vote_fraction${end}"
  echo
  echo "Feedback is always welcome via GitHub issues (${bblue}https://github.com/splitgraph/splitgraph/issues${end})"
  echo "or email (${bblue}support@splitgraph.com${end})!"
}

_get_binary_name
_install_binary
_setup_engine
_register_cloud
_welcome
