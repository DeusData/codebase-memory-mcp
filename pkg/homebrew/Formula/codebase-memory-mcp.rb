class CodebaseMemoryMcp < Formula
  desc "Fast code intelligence engine for AI coding agents"
  homepage "https://github.com/DeusData/codebase-memory-mcp"
  version "0.6.1"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-arm64.tar.gz"
      sha256 "3468e5df340e53dba2d23cc30bf44210c412dfb2538d7da992465934bf4980e0"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-amd64.tar.gz"
      sha256 "a85f8313181c7ffd3a0ac494c0e2bf1b5647f15de06686f8a0728162bf4326ac"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-arm64.tar.gz"
      sha256 "29811aadc2aa99fb9612683af30a06d03fcb2bfefb95c12ea672671827bd111c"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-amd64.tar.gz"
      sha256 "a24c04fd1fd61b08158e011685cf08d860e3594e4ce057ffda57d1cc2f4afdd1"
    end
  end

  def install
    bin.install "codebase-memory-mcp"
  end

  def caveats
    <<~EOS
      Run the following to configure your coding agents:
        codebase-memory-mcp install

      To tap this formula directly:
        brew tap deusdata/codebase-memory-mcp https://github.com/DeusData/codebase-memory-mcp
        brew install codebase-memory-mcp
    EOS
  end

  test do
    assert_match "codebase-memory-mcp", shell_output("#{bin}/codebase-memory-mcp --version")
  end
end
