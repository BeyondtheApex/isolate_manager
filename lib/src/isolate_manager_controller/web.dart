import 'dart:async';
import 'dart:js_interop';

import 'package:isolate_manager/isolate_manager.dart';
import 'package:isolate_manager/src/base/isolate_contactor.dart';
import 'package:isolate_manager/src/models/initial_params_mixin.dart';
import 'package:isolate_manager/src/utils/check_subtype.dart';
import 'package:isolate_manager/src/utils/extract_array_buffers.dart';
import 'package:web/web.dart';

/// This method only use to create a custom isolate.
class IsolateManagerControllerImpl<R, P>
    with InitialParamsMixin
    implements IsolateManagerController<R, P> {
  /// This method only use to create a custom isolate.
  ///
  /// The [params] is a default parameter of a custom isolate function.
  /// `onDispose` will be called when the controller is disposed.
  IsolateManagerControllerImpl(dynamic params, {void Function()? onDispose})
    : _delegate =
          params.runtimeType == DedicatedWorkerGlobalScope
              ? _IsolateManagerWorkerController<R, P>(
                params as DedicatedWorkerGlobalScope,
                onDispose: onDispose,
              )
              : IsolateContactorController<R, P>(params, onDispose: onDispose);

  /// Delegation of IsolateContactor.
  final IsolateContactorController<R, P> _delegate;

  /// Mark the isolate as initialized.
  ///
  /// This method is automatically applied when using `IsolateManagerFunction.customFunction`
  /// and `IsolateManagerFunction.workerFunction`.
  @override
  void initialized() => _delegate.initialized();

  /// Close this `IsolateManagerController`.
  @override
  Future<void> close() => _delegate.close();

  /// Get initial parameters when you create the IsolateManager.
  @override
  dynamic get initialParams => _delegate.initialParams;

  /// This parameter is only used for Isolate. Use to listen for values from the main application.
  @override
  Stream<P> get onIsolateMessage => _delegate.onIsolateMessage;

  /// Send values from Isolate to the main application (to `onMessage`).
  @override
  void sendResult(R result, {List<Object>? transferables}) =>
      _delegate.sendResult(result, transferables: transferables);

  /// Send the `Exception` to the main app.
  @override
  void sendResultError(IsolateException exception) =>
      _delegate.sendResultError(exception);

  /// Get direct access to the raw Worker global scope for advanced control.
  /// Only available in Web Worker environment.
  @override
  dynamic get rawWorkerScope {
    if (_delegate is _IsolateManagerWorkerController<R, P>) {
      return _delegate.rawWorkerScope;
    }
    throw UnsupportedError(
        'rawWorkerScope is only available in Web Worker environment');
  }

  /// Set a custom message handler that receives all raw messages.
  @override
  void setRawMessageHandler(bool Function(dynamic event) handler) {
    if (_delegate is _IsolateManagerWorkerController<R, P>) {
      _delegate.setRawMessageHandler(handler);
    } else {
      throw UnsupportedError(
          'setRawMessageHandler is only available in Web Worker environment');
    }
  }

  /// Send raw message directly through Worker's postMessage.
  @override
  void sendRawMessage(dynamic data) {
    if (_delegate is _IsolateManagerWorkerController<R, P>) {
      _delegate.sendRawMessage(data);
    } else {
      throw UnsupportedError(
          'sendRawMessage is only available in Web Worker environment');
    }
  }
}

// TODO(lamnhan066): Find a way to test these methods because it only used by the compiled JS Worker.
// coverage:ignore-start
class _IsolateManagerWorkerController<R, P>
    implements IsolateContactorController<R, P> {
  _IsolateManagerWorkerController(this.self, {this.onDispose}) {
    _originalOnMessage = (MessageEvent event) {
      // 先调用自定义的 rawMessageHandler（如果有）
      if (_rawMessageHandler != null) {
        final shouldContinue = _rawMessageHandler!(event);
        if (!shouldContinue) return; // 停止进一步处理
      }
      // 正常消息处理流程（上游风格）
      dynamic result = event.data.dartify();
      if (isImTypeSubtype<P>()) {
        result = ImType.wrap(result as Object);
      }
      _streamController.sink.add(result as P);
    }.toJS;

    self.onmessage = _originalOnMessage;
  }

  final DedicatedWorkerGlobalScope self;
  final void Function()? onDispose;
  final _streamController = StreamController<P>.broadcast();
  // 新增字段
  late final JSFunction _originalOnMessage;
  bool Function(dynamic)? _rawMessageHandler;

  /// 获取原始 Worker 作用域
  dynamic get rawWorkerScope => self;

  /// 设置原始消息处理器
  void setRawMessageHandler(bool Function(dynamic event) handler) {
    _rawMessageHandler = handler;
  }

  /// 发送原始消息
  void sendRawMessage(dynamic data) {
    try {
      // 安全地转换 data 为 JSAny
      JSAny? jsData;
      if (data == null) {
        jsData = null; // 直接使用 null
      } else if (data is String) {
        jsData = data.toJS;
      } else if (data is num) {
        jsData = data.toJS;
      } else if (data is bool) {
        jsData = data.toJS;
      } else {
        // 对于其他类型，尝试转换为字符串
        jsData = data.toString().toJS;
      }
      self.postMessage(jsData);
    } catch (e) {
      // 如果所有转换都失败，发送错误信息
      self.postMessage('Error converting data to JS: $e'.toJS);
    }
  }

  /// 新增：直接设置 onmessage 处理器（完全绕过 IsolateManager）
  void setRawOnMessageHandler(JSFunction handler) {
    self.onmessage = handler;
  }

  /// 新增：恢复默认的 onmessage 处理器
  void restoreDefaultOnMessageHandler() {
    self.onmessage = _originalOnMessage;
  }

  @override
  Stream<P> get onIsolateMessage => _streamController.stream.cast();

  @override
  Object? get initialParams => null;

  /// Send result to the main app
  @override
  void sendResult(R m, {List<Object>? transferables}) {
    final value = m is ImType ? m.unwrap : m;
    final payload = <String, Object?>{'type': 'data', 'value': value}.jsify();

    if (transferables != null && transferables.isNotEmpty) {
      // Extract ArrayBuffers from transferables for zero-copy transfer
      final jsTransferables = extractArrayBuffers(transferables);
      self.postMessage(payload, jsTransferables);
    } else {
      self.postMessage(payload);
    }
  }

  /// Send error to the main app
  @override
  void sendResultError(IsolateException exception) {
    self.postMessage(exception.toMap().jsify());
  }

  /// Mark the Worker as initialized
  @override
  void initialized() {
    self.postMessage(IsolateState.initialized.toMap().jsify());
  }

  /// Close this `IsolateManagerWorkerController`.
  @override
  Future<void> close() async {
    self.close();
  }

  @override
  Completer<void> get ensureInitialized => throw UnimplementedError();

  @override
  Stream<R> get onMessage => throw UnimplementedError();

  @override
  void sendIsolate(dynamic message, {List<Object>? transferables}) =>
      throw UnimplementedError();

  @override
  void sendIsolateState(IsolateState state) => throw UnimplementedError();
}

// coverage:ignore-end
