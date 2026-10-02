using System.Diagnostics;
using Avalonia;
using Avalonia.Collections;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Avalonia.VisualTree;
using Nuventra.NuvexaMQ.Server;

namespace Nuventra.NuvexaMQ.Desktop;

public sealed partial class MainWindow : Window
{
    private const int MaxLogLines = 2000;
    private const int MaxPendingLines = 8000;
    private readonly BrokerController _broker = new();
    private readonly HourlyLogWriter _logs = new();
    private readonly AvaloniaList<string> _logItems = new();
    private readonly Queue<PendingLog> _pending = new();
    private readonly object _logGate = new();
    private readonly DispatcherTimer _logTimer;
    private int _omitted;
    private int _skippedTotal;
    private DateTime _shownHour;
    private bool _ready;
    private bool _occupied;
    private ScrollViewer? _logScroller;
    private DesktopSettings _settings = new();

    private readonly record struct PendingLog(DateTime Hour, string Line);

    public MainWindow()
    {
        InitializeComponent();
        _logTimer = new DispatcherTimer(TimeSpan.FromMilliseconds(200), DispatcherPriority.Background, (_, _) => FlushLogs());
        _logTimer.Start();
        AttachCopyMenu(BrokerPreview, BrokerPreview.SelectAll, BrokerPreview.Copy, () => BrokerPreview.SelectionStart != BrokerPreview.SelectionEnd);
        AttachCopyMenu(HealthPreview, HealthPreview.SelectAll, HealthPreview.Copy, () => HealthPreview.SelectionStart != HealthPreview.SelectionEnd);
        AttachCopyMenu(HttpPreview, HttpPreview.SelectAll, HttpPreview.Copy, () => HttpPreview.SelectionStart != HttpPreview.SelectionEnd);
        AttachCopyMenu(HttpsPreview, HttpsPreview.SelectAll, HttpsPreview.Copy, () => HttpsPreview.SelectionStart != HttpsPreview.SelectionEnd);
        LogList.ItemsSource = _logItems;
        AttachCopyMenu(LogList, () => LogList.SelectAll(), CopyLog, () => LogList.SelectedItems?.Count > 0);
        _broker.Logged += OnLogged;
        _settings = DesktopSettings.Load();
        BrokerPortBox.Text = _settings.BrokerPort.ToString();
        HealthPortBox.Text = _settings.HealthPort.ToString();
        HttpPortBox.Text = _settings.AdminHttpPort.ToString();
        HttpsPortBox.Text = _settings.AdminHttpsPort.ToString();
        DataDirBox.Text = string.IsNullOrWhiteSpace(_settings.DataDir) ? DesktopSettings.DefaultDataDir() : _settings.DataDir;
        LogsPathBox.Text = string.IsNullOrWhiteSpace(_settings.LogsDir) ? DesktopSettings.DefaultLogsDir() : _settings.LogsDir;
        VerboseBox.IsChecked = _settings.Verbose;
        LoggingBox.IsChecked = _settings.EnableLogging;
        _shownHour = HourlyLogWriter.HourOf(DateTime.Now);
        _ready = true;
        ApplyLogging();
        ShowCurrentHour();
        ApplyState(running: false);
        RefreshPreviews();
        if (!string.IsNullOrWhiteSpace(CommandPath.Note))
            Append(CommandPath.Note);
    }

    private async void StartClick(object? sender, RoutedEventArgs e)
    {
        if (!TryReadSettings(out var settings, out var error))
        {
            Append(error);
            return;
        }

        _settings = settings;
        _settings.Save();
        ApplyLogging();
        ApplyState(running: false, busy: true);
        StatusText.Text = "Starting";
        try
        {
            await _broker.StartAsync(settings);
            _occupied = false;
            ApplyState(running: true);
        }
        catch (Exception ex)
        {
            Append(ex.Message);
            _occupied = ExternalBroker.IsAddressInUse(ex);
            ApplyState(running: false, occupied: _occupied);
        }
    }

    private async void StopClick(object? sender, RoutedEventArgs e)
    {
        var occupied = _occupied;
        StatusText.Text = "Stopping";
        ApplyState(running: _broker.IsRunning, busy: true, occupied: occupied);
        try
        {
            if (occupied && await ExternalBroker.MacServiceLoadedAsync())
            {
                Append("Stopping the NuvexaMQ service. macOS may ask for your password.");
                var error = await ExternalBroker.BootoutMacServiceAsync();
                if (error is not null)
                    throw new InvalidOperationException(error);
            }

            await _broker.StopAsync();
            if (occupied)
                await ExternalBroker.ReleaseAsync(ListenerPorts(), LogUi);
            _occupied = false;
        }
        catch (Exception ex)
        {
            Append(ex.Message);
            _occupied = occupied;
        }

        ApplyState(running: _broker.IsRunning, occupied: _occupied);
    }

    private void OpenClick(object? sender, RoutedEventArgs e)
    {
        var url = HttpPreview.Text ?? "";
        if (url.Length == 0)
            return;
        Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
    }

    private void PortChanged(object? sender, TextChangedEventArgs e) => RefreshPreviews();

    private void LoggingChanged(object? sender, RoutedEventArgs e)
    {
        if (!_ready || !TryReadSettings(out var settings, out _))
            return;
        _settings = settings;
        settings.Save();
        ApplyLogging();
    }

    private void OnLogged(string line)
    {
        var now = DateTime.Now;
        _logs.Write(now, line);
        lock (_logGate)
        {
            _pending.Enqueue(new PendingLog(HourlyLogWriter.HourOf(now), line));
            while (_pending.Count > MaxPendingLines)
            {
                _pending.Dequeue();
                _omitted++;
            }
        }
    }

    private void SelectUrlOnClick(object? sender, PointerReleasedEventArgs e)
    {
        if (sender is not SelectableTextBlock block || block.SelectionStart != block.SelectionEnd)
            return;
        block.SelectAll();
    }

    private void LogUi(string line)
    {
        if (Dispatcher.UIThread.CheckAccess())
            Append(line);
        else
            Dispatcher.UIThread.Post(() => Append(line));
    }

    private void Append(string line)
    {
        var now = DateTime.Now;
        _logs.Write(now, line);
        RollConsole(now);
        AddLines([line], 0);
    }

    private void FlushLogs()
    {
        var now = DateTime.Now;
        _logs.Roll(now);
        var rolled = RollConsole(now);
        PendingLog[] batch;
        int omitted;
        lock (_logGate)
        {
            if (rolled)
                _omitted = 0;
            if (_pending.Count == 0 && _omitted == 0)
                return;
            batch = _pending.ToArray();
            _pending.Clear();
            omitted = _omitted;
            _omitted = 0;
        }

        var hour = _shownHour;
        var lines = new List<string>(batch.Length);
        foreach (var item in batch)
        {
            if (item.Hour == hour)
                lines.Add(item.Line);
        }

        AddLines(lines, omitted);
    }

    private bool RollConsole(DateTime local)
    {
        var hour = HourlyLogWriter.HourOf(local);
        if (hour == _shownHour)
            return false;
        _shownHour = hour;
        _logItems.Clear();
        _skippedTotal = 0;
        return true;
    }

    private void ShowCurrentHour()
    {
        var (skipped, lines) = _logs.Tail(MaxLogLines);
        _shownHour = HourlyLogWriter.HourOf(DateTime.Now);
        _logItems.Clear();
        _skippedTotal = 0;
        if (lines.Length > 0 || skipped > 0)
            AddLines(lines, skipped);
    }

    private async void BrowseLogsClick(object? sender, RoutedEventArgs e)
    {
        var options = new FolderPickerOpenOptions
        {
            Title = "Choose log folder",
            AllowMultiple = false
        };
        var current = LogsDirectory();
        if (Directory.Exists(current))
        {
            var folder = await StorageProvider.TryGetFolderFromPathAsync(current);
            if (folder is not null)
                options.SuggestedStartLocation = folder;
        }

        var picked = await StorageProvider.OpenFolderPickerAsync(options);
        if (picked.Count == 0)
            return;
        var path = picked[0].TryGetLocalPath();
        if (string.IsNullOrWhiteSpace(path))
            return;
        LogsPathBox.Text = path;
        ApplyLogging(reloadConsole: true);
        SaveSettings();
    }

    private void LogsPathLostFocus(object? sender, RoutedEventArgs e)
    {
        if (!_ready)
            return;
        ApplyLogging(reloadConsole: true);
        SaveSettings();
    }

    private void ApplyLogging(bool reloadConsole = false)
    {
        _logs.Configure(LogsDirectory(), LoggingBox.IsChecked == true);
        if (reloadConsole)
            ShowCurrentHour();
    }

    private string LogsDirectory()
    {
        var path = (LogsPathBox.Text ?? "").Trim();
        return path.Length == 0 ? DesktopSettings.DefaultLogsDir() : path;
    }

    private void SaveSettings()
    {
        if (!TryReadSettings(out var settings, out _))
            return;
        _settings = settings;
        settings.Save();
    }

    private void AddLines(IReadOnlyList<string> batch, int omitted)
    {
        var stick = StickToEnd();
        if (batch.Count > MaxLogLines)
        {
            omitted += batch.Count - MaxLogLines;
            batch = batch.Skip(batch.Count - MaxLogLines).ToArray();
        }

        if (batch.Count > 0)
            _logItems.AddRange(batch);
        var extra = _logItems.Count - MaxLogLines;
        if (extra > 0)
        {
            omitted += extra;
            _logItems.RemoveRange(0, extra);
        }

        if (omitted > 0)
        {
            _skippedTotal += omitted;
            var note = $"… {_skippedTotal:N0} earlier log lines skipped";
            if (_logItems.Count > 0 && _logItems[0].StartsWith("… ", StringComparison.Ordinal))
                _logItems[0] = note;
            else
                _logItems.Insert(0, note);
            extra = _logItems.Count - MaxLogLines;
            if (extra > 0)
                _logItems.RemoveRange(1, extra);
        }

        if (stick && _logItems.Count > 0)
            LogList.ScrollIntoView(_logItems[^1]);
    }

    private bool StickToEnd()
    {
        var scroller = LogScroller();
        if (scroller is null)
            return true;
        var max = scroller.Extent.Height - scroller.Viewport.Height;
        return max <= 1 || scroller.Offset.Y >= max - 24;
    }

    private ScrollViewer? LogScroller() =>
        _logScroller ??= LogList.GetVisualDescendants().OfType<ScrollViewer>().FirstOrDefault();

    private void CopyLog()
    {
        var selected = LogList.SelectedItems?.OfType<string>().ToArray();
        var text = selected is { Length: > 0 }
            ? string.Join(Environment.NewLine, selected)
            : string.Join(Environment.NewLine, _logItems);
        _ = TopLevel.GetTopLevel(this)?.Clipboard?.SetTextAsync(text);
    }

    private static void AttachCopyMenu(Control control, Action selectAll, Action copy, Func<bool> hasSelection)
    {
        var copyItem = new MenuItem { Header = "Copy" };
        copyItem.Click += (_, _) =>
        {
            if (!hasSelection())
                selectAll();
            copy();
        };
        var selectAllItem = new MenuItem { Header = "Select all" };
        selectAllItem.Click += (_, _) => selectAll();
        var menu = new ContextMenu();
        menu.Items.Add(copyItem);
        menu.Items.Add(selectAllItem);
        control.ContextMenu = menu;
    }

    private void RefreshPreviews()
    {
        BrokerPreview.Text = Preview(BrokerPortBox.Text, port => $"tcp://127.0.0.1:{port}");
        HealthPreview.Text = Preview(HealthPortBox.Text, port => $"http://127.0.0.1:{port}/health");
        HttpPreview.Text = Preview(HttpPortBox.Text, port => $"http://127.0.0.1:{port}/");
        HttpsPreview.Text = Preview(HttpsPortBox.Text, port => $"https://127.0.0.1:{port}/");
    }

    private static string Preview(string? text, Func<int, string> format) =>
        int.TryParse(text, out var port) && port is > 0 and <= 65535 ? format(port) : "";

    private bool TryReadSettings(out DesktopSettings settings, out string error)
    {
        settings = new DesktopSettings();
        if (!ReadPort(BrokerPortBox.Text, "Broker port", out var broker, out error)
            || !ReadPort(HealthPortBox.Text, "Health port", out var health, out error)
            || !ReadPort(HttpPortBox.Text, "Admin HTTP port", out var http, out error)
            || !ReadPort(HttpsPortBox.Text, "Admin HTTPS port", out var https, out error))
            return false;

        if (new[] { broker, health, http, https }.Distinct().Count() != 4)
        {
            error = "Broker, health, admin HTTP, and admin HTTPS ports must be different.";
            return false;
        }

        var dataDir = (DataDirBox.Text ?? "").Trim();
        if (dataDir.Length == 0)
            dataDir = DesktopSettings.DefaultDataDir();

        settings.BrokerPort = broker;
        settings.HealthPort = health;
        settings.AdminHttpPort = http;
        settings.AdminHttpsPort = https;
        settings.DataDir = dataDir;
        settings.Verbose = VerboseBox.IsChecked == true;
        settings.EnableLogging = LoggingBox.IsChecked == true;
        settings.LogsDir = LogsDirectory();
        error = "";
        return true;
    }

    private static bool ReadPort(string? text, string label, out int port, out string error)
    {
        if (!int.TryParse(text, out port) || port is < 1 or > 65535)
        {
            error = $"{label} must be from 1 to 65535.";
            return false;
        }

        error = "";
        return true;
    }

    private int[] ListenerPorts() =>
    [
        _settings.BrokerPort,
        _settings.HealthPort,
        _settings.AdminHttpPort,
        _settings.AdminHttpsPort
    ];

    private void ApplyState(bool running, bool busy = false, bool occupied = false)
    {
        var live = running || occupied;
        if (!busy)
            StatusText.Text = running ? "Running" : occupied ? "In use" : "Stopped";
        StartButton.IsEnabled = !live && !busy;
        StopButton.IsEnabled = live && !busy;
        OpenButton.IsEnabled = live && !busy;
        var editable = !live && !busy;
        BrokerPortBox.IsEnabled = editable;
        HealthPortBox.IsEnabled = editable;
        HttpPortBox.IsEnabled = editable;
        HttpsPortBox.IsEnabled = editable;
        DataDirBox.IsEnabled = editable;
        LogsPathBox.IsEnabled = editable;
        BrowseLogsButton.IsEnabled = editable;
        VerboseBox.IsEnabled = editable;
    }

    protected override async void OnClosed(EventArgs e)
    {
        _logTimer.Stop();
        if (TryReadSettings(out var settings, out _))
            settings.Save();
        _logs.Dispose();
        await _broker.DisposeAsync();
        base.OnClosed(e);
    }
}
