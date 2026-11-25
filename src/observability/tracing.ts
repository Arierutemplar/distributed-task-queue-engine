import {
  context,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
} from '@opentelemetry/api';
import type { RequestHandler } from 'express';
import type { TaskJobMeta } from '../types/task.types.js';

// Without a registered provider every call below is a cheap no-op, so tracing stays
// non-intrusive until tracing is enabled with an explicit endpoint.
const tracer = trace.getTracer('distributed-task-queue');
const jobAttemptContexts = new WeakMap<object, Pick<TaskJobMeta, 'traceparent' | 'tracestate'>>();

export interface TracingSettings {
  enabled: boolean;
  endpoint?: string;
  serviceName: string;
  role: string;
}

export interface TracingHandle {
  shutdown: () => Promise<void>;
}

/** Registers an OTLP/HTTP exporter; SDK packages are only loaded when tracing is enabled. */
export const initTracing = async (
  settings: TracingSettings,
): Promise<TracingHandle | undefined> => {
  if (!settings.enabled || !settings.endpoint) {
    return undefined;
  }
  const [
    { NodeTracerProvider },
    { BatchSpanProcessor },
    { OTLPTraceExporter },
    { resourceFromAttributes },
    { ATTR_SERVICE_NAME },
  ] = await Promise.all([
    import('@opentelemetry/sdk-trace-node'),
    import('@opentelemetry/sdk-trace-base'),
    import('@opentelemetry/exporter-trace-otlp-http'),
    import('@opentelemetry/resources'),
    import('@opentelemetry/semantic-conventions'),
  ]);

  const endpoint = new URL(settings.endpoint);
  endpoint.pathname = `${endpoint.pathname.replace(/\/+$/u, '')}/v1/traces`;
  const tracesEndpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: settings.serviceName,
      'service.role': settings.role,
    }),
    spanProcessors: [
      new BatchSpanProcessor(
        new OTLPTraceExporter({
          url: tracesEndpoint?.trim() ? tracesEndpoint : endpoint.toString(),
        }),
      ),
    ],
  });
  // Installs the AsyncLocalStorage context manager and the W3C trace-context propagator.
  provider.register();
  return { shutdown: () => provider.shutdown() };
};

/** Server span per request, continuing an inbound `traceparent` when present. */
export const traceRequests: RequestHandler = (request, response, next) => {
  const parent = propagation.extract(context.active(), request.headers);
  const span = tracer.startSpan(
    `${request.method} ${request.baseUrl || request.path}`,
    {
      kind: SpanKind.SERVER,
      attributes: {
        'http.request.method': request.method,
        'url.path': request.path,
        ...(typeof response.locals.requestId === 'string'
          ? { 'request.id': response.locals.requestId }
          : {}),
      },
    },
    parent,
  );
  response.once('finish', () => {
    const routePath = (request.route as { path?: unknown } | undefined)?.path;
    if (typeof routePath === 'string') {
      const route = `${request.baseUrl}${routePath}`;
      span.updateName(`${request.method} ${route}`);
      span.setAttribute('http.route', route);
    }
    span.setAttribute('http.response.status_code', response.statusCode);
    if (response.statusCode >= 500) {
      span.setStatus({ code: SpanStatusCode.ERROR });
    }
    span.end();
  });
  context.with(trace.setSpan(parent, span), next);
};

/** Trace context to persist with a job so the worker span joins the request trace. */
export const currentTraceContext = (): Pick<TaskJobMeta, 'traceparent' | 'tracestate'> => {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return {
    ...(carrier.traceparent ? { traceparent: carrier.traceparent } : {}),
    ...(carrier.tracestate ? { tracestate: carrier.tracestate } : {}),
  };
};

export const rememberJobAttempt = (job: object): void => {
  const carrier = currentTraceContext();
  if (carrier.traceparent) jobAttemptContexts.set(job, carrier);
};

export const jobAttemptContext = (
  job: object,
): Pick<TaskJobMeta, 'traceparent' | 'tracestate'> | undefined => jobAttemptContexts.get(job);

const extractJobContext = (meta: Pick<TaskJobMeta, 'traceparent' | 'tracestate'> | undefined) =>
  propagation.extract(ROOT_CONTEXT, {
    ...(meta?.traceparent ? { traceparent: meta.traceparent } : {}),
    ...(meta?.tracestate ? { tracestate: meta.tracestate } : {}),
  });

export const withTraceContext = <T>(
  meta: Pick<TaskJobMeta, 'traceparent' | 'tracestate'> | undefined,
  operation: () => T,
): T => context.with(extractJobContext(meta), operation);

/** Runs a job attempt inside a consumer span whose parent is the stored trace context. */
export const withJobSpan = <T>(
  name: string,
  meta: Pick<TaskJobMeta, 'traceparent' | 'tracestate'> | undefined,
  attributes: Attributes,
  operation: () => Promise<T>,
  kind: SpanKind = SpanKind.CONSUMER,
): Promise<T> => {
  const parent = extractJobContext(meta);
  return tracer.startActiveSpan(name, { kind, attributes }, parent, async (span) => {
    try {
      return await operation();
    } catch (error: unknown) {
      span.recordException(error instanceof Error ? error : String(error));
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw error;
    } finally {
      span.end();
    }
  });
};

/** Runs an operation as a child of the current span. */
export const withSpan = <T>(
  name: string,
  kind: SpanKind,
  attributes: Attributes,
  operation: () => Promise<T>,
): Promise<T> =>
  tracer.startActiveSpan(name, { kind, attributes }, async (span) => {
    try {
      return await operation();
    } catch (error: unknown) {
      span.recordException(error instanceof Error ? error : String(error));
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw error;
    } finally {
      span.end();
    }
  });

/** Log correlation fields for the active span, if any. */
export const activeTraceFields = (): { traceId?: string; spanId?: string } => {
  const spanContext = trace.getSpanContext(context.active());
  if (!spanContext || !trace.isSpanContextValid(spanContext)) {
    return {};
  }
  return { traceId: spanContext.traceId, spanId: spanContext.spanId };
};
