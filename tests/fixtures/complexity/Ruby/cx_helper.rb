module CxApp
  class CxBase
    def label
      "base"
    end
  end

  class CxHelper
    def assist(text)
      CxWidget.new.name + text
    end
  end
end
