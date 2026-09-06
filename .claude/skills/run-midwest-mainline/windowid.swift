// Prints the CGWindowID of the first on-screen window owned by the process named in argv[1],
// for `screencapture -l<id>`. Needs no accessibility permission.
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
    if name == owner && layer == 0 && height > 50, let id = window[kCGWindowNumber as String] as? Int {
        print(id)
        exit(0)
    }
}
exit(1)
