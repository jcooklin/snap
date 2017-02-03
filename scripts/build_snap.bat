@echo off
setlocal
echo Building Snap
set _proj_dir=%~dp0..
for /f "tokens=1-3" %%i in ('git --version') do set git_version=%%k
set go_build=go build -ldflags "-w -X main.gitversion=%git_version%"
set CGO_ENABLED=0
set GOOS=windows
reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set GOARCH=386 || set GOARCH=amd64

if %GOARCH%==amd64 (
	set build_path=%_proj_dir%\build\windows\x86_64
) else (
	set build_path=%_proj_dir%\build\windows\amd64
)

md %build_path%

cd /D %_proj_dir%
%go_build% -o "%build_path%\snapteld.exe" snapteld.go || exit /B 1

cd /D %_proj_dir%\cmd\snaptel 
%go_build% -o "%build_path%\snaptel.exe" . || exit /B 1

echo Finished building Snap
endlocal