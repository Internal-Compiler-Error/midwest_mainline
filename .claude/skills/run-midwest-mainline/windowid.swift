// Prints the CGWindowID of the first on-screen window owned by the process named in argv[1],
// for `screencapture -l<id>`. Needs no accessibility permission. Exits 3 when the window is
// open but not on the current Space, 1 when there's no window at all.
import CoreGraphics
import Foundation

let owner = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "downloader-gui"
let options: CGWindowListOption = [.optionOnScreenOnly, .excludeDesktopElements]
guard let windows = CGWindowListCopyWindowInfo(options, kCGNullWindowID) as? [[String: Any]] else {
    exit(2)
}
if owner == "--list" {
    for window in windows {
        let name = window[kCGWindowOwnerName as String] as? String ?? ""
        let layer = window[kCGWindowLayer as String] as? Int ?? 0
        let bounds = window[kCGWindowBounds as String] as? [String: Any] ?? [:]
        print(name, layer, bounds["Width"] ?? 0, bounds["Height"] ?? 0)
    }
    exit(0)
}
for window in windows {
    let name = window[kCGWindowOwnerName as String] as? String ?? ""
    let layer = window[kCGWindowLayer as String] as? Int ?? 0
    let bounds = window[kCGWindowBounds as String] as? [String: Any] ?? [:]
    let height = bounds["Height"] as? Double ?? 0
    // layer 0 is a normal window, 5 one kept on top (DOWNLOADER_WINDOW_ON_TOP)
    if name == owner && (0...5).contains(layer) && height > 50, let id = window[kCGWindowNumber as String] as? Int {
        print(id)
        exit(0)
    }
}
// not on screen: on another Space (the user is in a full-screen app, say), or not open yet
let everywhere = CGWindowListCopyWindowInfo([.optionAll], kCGNullWindowID) as? [[String: Any]] ?? []
if everywhere.contains(where: { ($0[kCGWindowOwnerName as String] as? String) == owner && ($0[kCGWindowLayer as String] as? Int ?? 0) == 5 }) {
    exit(3)
}
exit(1)
