import * as React from "react";
import {
  PresenceClient,
  type PresenceClientOptions,
  type PresenceConnectionState,
  type PresencePeer,
} from "./Client.ts";
import type {
  ConnectionStatus,
  MessageRef,
} from "../client/status.ts";

export interface UsePresenceResult<TState> {
  readonly peers: ReadonlyMap<string, TState>;
  readonly peerList: readonly PresencePeer<TState>[];
  readonly self: string | undefined;
  readonly state: PresenceConnectionState;
  /**
   * Two-way connection status — drives offline / syncing / online
   * indicators. See {@link ConnectionStatus} for the full enum.
   */
  readonly status: ConnectionStatus;
  readonly update: (state: TState) => void;
  readonly send: (data: unknown) => void;
  readonly sendWithStatus: (data: unknown) => MessageRef<unknown>;
  readonly retry: (id: string) => void;
  readonly cancel: (id: string) => void;
  readonly outbox: ReadonlyArray<MessageRef<unknown>>;
  readonly client: PresenceClient<TState, unknown>;
}

const emptyOutbox: ReadonlyArray<MessageRef<unknown>> = Object.freeze([]);

/**
 * Subscribe a component to a Presence room. The hook creates a
 * {@link PresenceClient} on mount and tears it down on unmount.
 *
 * @typeParam TState - The presence state shape.
 * @typeParam TMessage - Optional broadcast message shape; defaults to `unknown`.
 */
export function usePresence<TState, TMessage = unknown>(
  options: PresenceClientOptions<TState, TMessage>,
): UsePresenceResult<TState> {
  const clientRef = React.useRef<PresenceClient<TState, TMessage> | null>(null);
  if (clientRef.current === null) {
    clientRef.current = new PresenceClient<TState, TMessage>(options);
  }
  const client = clientRef.current;

  const [peers, setPeers] = React.useState<ReadonlyMap<string, TState>>(
    () => client.peers,
  );
  const [self, setSelf] = React.useState<string | undefined>(() => client.self);
  const [state, setState] = React.useState<PresenceConnectionState>(
    () => client.state,
  );
  const [status, setStatus] = React.useState<ConnectionStatus>(
    () => client.status,
  );
  const [outbox, setOutbox] =
    React.useState<ReadonlyArray<MessageRef<unknown>>>(() =>
      client.outbox.length > 0 ? client.outbox : emptyOutbox,
    );

  React.useEffect(() => {
    clientRef.current = client;
    const onPeers = (next: ReadonlyMap<string, TState>) => setPeers(next);
    const onSelf = (id: string) => setSelf(id);
    const onState = (_s: PresenceConnectionState) => {
      setState(client.state);
      setStatus(client.status);
    };
    const onOutbox = (next: ReadonlyArray<MessageRef<TMessage>>) =>
      setOutbox(next);

    (client as unknown as {
      onPeers?: (p: ReadonlyMap<string, TState>) => void;
    }).onPeers = onPeers;
    (client as unknown as { onSelf?: (id: string) => void }).onSelf = onSelf;
    (client as unknown as {
      onState?: (s: PresenceConnectionState) => void;
    }).onState = onState;

    const unsubOutbox = client.subscribeOutbox(onOutbox);
    client.connect();

    return () => {
      unsubOutbox();
      client.close();
      clientRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return React.useMemo(
    () => ({
      peers,
      peerList: [...peers.entries()].map(([id, peerState]) => ({
        id,
        state: peerState,
      })),
      self,
      state,
      status,
      update: (next: TState) => client.update(next),
      send: (data: unknown) => client.send(data as TMessage),
      sendWithStatus: (data: unknown) => client.sendWithStatus(data as TMessage),
      retry: (id: string) => client.retry(id),
      cancel: (id: string) => client.cancel(id),
      outbox,
      client: client as unknown as PresenceClient<TState, unknown>,
    }),
    [peers, self, state, status, outbox, client],
  );
}