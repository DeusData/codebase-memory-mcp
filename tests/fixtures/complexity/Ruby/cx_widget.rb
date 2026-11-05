# Rails-shaped pair: no require, the constant is found through the global
# namespace, so every call below is a cross-file resolution.
module CxApp
  class CxWidget < CxBase
    def render
      helper = CxHelper.new
      helper.assist(name)
    end

    def name
      "widget"
    end
  end
end
