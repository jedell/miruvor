#!/bin/sh
osascript - "$@" <<EOF
on run argv
    repeat with i from 1 to (count argv)
        tell application "iTerm"
            activate
            set new_term to (create window with default profile)
            tell new_term
                tell the current session
                    write text "PORT=" & 4000 + i & " iex --name " & item i of argv & "@127.0.0.1 -S mix phx.server"
                end tell
            end tell
        end tell
    end repeat
end run
EOF