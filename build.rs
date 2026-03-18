fn main() {
    let config = slint_build::CompilerConfiguration::new().with_style("fluent".into());
    slint_build::compile_with_config("ui/app.slint", config).expect("failed to compile Slint UI");

    if cfg!(target_os = "windows") {
        let manifest = r#"
            <assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
              <assemblyIdentity version="1.0.0.0" processorArchitecture="*" name="MoonIngest" type="win32"/>
              <description>FastTrack</description>
              <application xmlns="urn:schemas-microsoft-com:asm.v3">
                <windowsSettings>
                  <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true</dpiAware>
                  <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2</dpiAwareness>
                  <longPathAware xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">true</longPathAware>
                </windowsSettings>
              </application>
            </assembly>
        "#;

        let mut resource = winresource::WindowsResource::new();
        resource.set_manifest(manifest);
        let _ = resource.compile();
    }
}
