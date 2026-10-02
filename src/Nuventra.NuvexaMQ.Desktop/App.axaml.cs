using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Nuventra.NuvexaMQ.Server;

namespace Nuventra.NuvexaMQ.Desktop;

public sealed class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            try
            {
                var result = CommandPath.Install(AppContext.BaseDirectory);
                CommandPath.Note = result.Changed ? result.Message : null;
            }
            catch (Exception ex)
            {
                CommandPath.Note = ex.Message;
            }

            desktop.MainWindow = new MainWindow();
        }

        base.OnFrameworkInitializationCompleted();
    }
}
