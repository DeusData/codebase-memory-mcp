class Box {
    init(value: Int) {}
    init(name: String) {}
    func read(value: Int) -> Int { value }
    func read(name: String) -> String { name }
}
func run() {
    Box.init(value: 1)
    Box.init(name: "x")
    let box: Box = Box(value: 1)
    box.read(value: 1)
    box.read(name: "x")
}
func extensionCaller(box: Box) {
    box.extended(value: 1)
    _ = Box.init(flag: true)
}
