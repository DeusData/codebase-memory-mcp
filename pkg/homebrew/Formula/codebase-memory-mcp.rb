class CodebaseMemoryMcp < Formula
  desc "Fast code intelligence engine for AI coding agents"
  homepage "https://github.com/DeusData/codebase-memory-mcp"
  version "0.6.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-arm64.tar.gz"
      sha256 "a1d3f8a4c353ab94ea8fe1fb60159758020f2f256c9652699a0bd6725189a439"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-amd64.tar.gz"
      sha256 "a4d09d97fe1f47e1a0a23309bc34d9937f74c61950bed3259f9576800cc78727"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-arm64.tar.gz"
      sha256 "f1fad27262fe7af4a356af128e43942355cb2189491079b6790ecc5ae3af069c"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-amd64.tar.gz"
      sha256 "0dfd70f73337219925f3ec6a572fe776dbbe1c4c8c6ab546ab214fe16e56a426"
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
