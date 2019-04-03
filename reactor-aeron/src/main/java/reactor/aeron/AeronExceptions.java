package reactor.aeron;

class AeronExceptions {

  private AeronExceptions() {
    // Do not instantiate
  }

  static RuntimeException failWithCancel(String message) {
    return new AeronCancelException(message);
  }

  static RuntimeException failWithEventLoopUnavailable() {
    return new AeronEventLoopException("AeronEventLoop is unavailable");
  }

  static RuntimeException failWithPublication(String message) {
    return new AeronPublicationException(message);
  }

  static RuntimeException failWithResourceDisposal(String resourceName) {
    return new AeronResourceDisposalException(
        "Can only close resource (" + resourceName + ") from within event loop");
  }

  static class AeronCancelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    AeronCancelException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class AeronEventLoopException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    AeronEventLoopException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class AeronPublicationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    AeronPublicationException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static class AeronResourceDisposalException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    AeronResourceDisposalException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }
}
