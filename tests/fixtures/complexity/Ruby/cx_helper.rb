module CxApp
  class CxBase
    def label
      "base"
    end
  end

  class CxHelper
    def assist(text)
      widget = CxWidget.new
      widget.label
      widget.name + text
    end
  end
end
