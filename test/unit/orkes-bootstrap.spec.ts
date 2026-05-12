import { promises as fs } from 'fs';
import { OrkesBootstrapService } from '../../src/orkes/orkes-bootstrap.service';
import type { AppConfigService } from '../../src/config/app-config.service';
import type { OrkesClients } from '@io-orkes/conductor-javascript';

interface MockEvents {
  getEventHandlerByName: jest.Mock;
  addEventHandler: jest.Mock;
  updateEventHandler: jest.Mock;
}

interface MockMetadata {
  registerWorkflowDef: jest.Mock;
}

const buildClients = (): {
  clients: OrkesClients;
  metadata: MockMetadata;
  events: MockEvents;
} => {
  const metadata: MockMetadata = {
    registerWorkflowDef: jest.fn(async () => undefined),
  };
  const events: MockEvents = {
    getEventHandlerByName: jest.fn(async () => {
      throw new Error('not found');
    }),
    addEventHandler: jest.fn(async () => undefined),
    updateEventHandler: jest.fn(async () => undefined),
  };
  const clients = {
    getMetadataClient: () => metadata,
    getEventClient: () => events,
  } as unknown as OrkesClients;
  return { clients, metadata, events };
};

const buildCfg = (overrides: Partial<{ autoRegister: boolean }> = {}): AppConfigService =>
  ({
    orkes: {
      enabled: true,
      autoRegister: overrides.autoRegister ?? true,
      serverUrl: 'https://developer.orkescloud.com/api',
      key: 'k',
      secret: 's',
    },
  }) as unknown as AppConfigService;

describe('OrkesBootstrapService', () => {
  afterEach(() => jest.restoreAllMocks());

  it('no-ops when clients is null', async () => {
    const cfg = buildCfg();
    const svc = new OrkesBootstrapService(null, cfg);
    await expect(svc.onApplicationBootstrap()).resolves.toBeUndefined();
  });

  it('no-ops when ORKES_AUTO_REGISTER is false', async () => {
    const { clients, metadata, events } = buildClients();
    const cfg = buildCfg({ autoRegister: false });
    const svc = new OrkesBootstrapService(clients, cfg);
    await svc.onApplicationBootstrap();
    expect(metadata.registerWorkflowDef).not.toHaveBeenCalled();
    expect(events.addEventHandler).not.toHaveBeenCalled();
  });

  it('registers workflows + adds event handler from JSON files', async () => {
    const { clients, metadata, events } = buildClients();
    const cfg = buildCfg();
    const svc = new OrkesBootstrapService(clients, cfg);

    const workflowJson = JSON.stringify({
      name: 'kafka_demo_workflow',
      version: 1,
      schemaVersion: 2,
      tasks: [],
    });
    const handlerJson = JSON.stringify({
      name: 'order_placed_handler',
      event: 'kafka:one-bth-dev-order-placed-in-private:orkes-demo-handler',
      active: true,
      actions: [],
    });

    jest.spyOn(fs, 'access').mockResolvedValue(undefined);
    jest
      .spyOn(fs, 'readdir')
      .mockImplementation(async (dir: unknown) => {
        const path = String(dir);
        if (path.endsWith('workflows')) return ['kafka_demo_workflow.json'] as never;
        if (path.endsWith('event_handlers')) return ['order_placed_handler.json'] as never;
        return [] as never;
      });
    jest
      .spyOn(fs, 'readFile')
      .mockImplementation(async (path: unknown) => {
        const p = String(path);
        if (p.endsWith('kafka_demo_workflow.json')) return workflowJson;
        if (p.endsWith('order_placed_handler.json')) return handlerJson;
        return '';
      });

    jest
      .spyOn((svc as unknown as { logger: { log: jest.Mock; error: jest.Mock } }).logger, 'log')
      .mockImplementation(() => undefined);

    await svc.onApplicationBootstrap();

    expect(metadata.registerWorkflowDef).toHaveBeenCalledTimes(1);
    expect(metadata.registerWorkflowDef.mock.calls[0][0]).toMatchObject({
      name: 'kafka_demo_workflow',
      version: 1,
    });
    expect(metadata.registerWorkflowDef.mock.calls[0][1]).toBe(true);

    expect(events.addEventHandler).toHaveBeenCalledTimes(1);
    expect(events.addEventHandler.mock.calls[0][0]).toMatchObject({
      name: 'order_placed_handler',
    });
    expect(events.updateEventHandler).not.toHaveBeenCalled();
  });

  it('updates the event handler when one with the same name already exists', async () => {
    const { clients, metadata, events } = buildClients();
    events.getEventHandlerByName = jest.fn(async () => ({ name: 'order_placed_handler' }));

    const cfg = buildCfg();
    const svc = new OrkesBootstrapService(clients, cfg);

    jest.spyOn(fs, 'access').mockResolvedValue(undefined);
    jest
      .spyOn(fs, 'readdir')
      .mockImplementation(async (dir: unknown) => {
        const p = String(dir);
        if (p.endsWith('event_handlers')) return ['h.json'] as never;
        return [] as never;
      });
    jest
      .spyOn(fs, 'readFile')
      .mockResolvedValue(
        JSON.stringify({
          name: 'order_placed_handler',
          event: 'kafka:t:g',
          active: true,
          actions: [],
        }),
      );

    jest
      .spyOn(
        (svc as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);

    await svc.onApplicationBootstrap();

    expect(events.updateEventHandler).toHaveBeenCalledTimes(1);
    expect(events.addEventHandler).not.toHaveBeenCalled();
    expect(metadata.registerWorkflowDef).not.toHaveBeenCalled();
  });
});
