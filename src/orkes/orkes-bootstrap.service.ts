import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { promises as fs } from 'fs';
import { join } from 'path';
import type {
  EventHandler,
  OrkesClients,
  WorkflowDef,
} from '@io-orkes/conductor-javascript';
import { AppConfigService } from '../config/app-config.service';
import { ORKES_CLIENTS } from './orkes.tokens';

/**
 * Loads workflow + event-handler definitions from `orkes/` and registers them
 * with Orkes Conductor at boot when `ORKES_AUTO_REGISTER=true`. No-ops when
 * Orkes is disabled or `OrkesClients` could not be constructed.
 */
@Injectable()
export class OrkesBootstrapService implements OnApplicationBootstrap {
  private readonly logger = new Logger(OrkesBootstrapService.name);

  constructor(
    @Inject(ORKES_CLIENTS) private readonly clients: OrkesClients | null,
    private readonly cfg: AppConfigService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    if (!this.clients) return;
    if (!this.cfg.orkes.autoRegister) {
      this.logger.log('ORKES_AUTO_REGISTER=false; skipping bootstrap');
      return;
    }

    const root = await this.resolveOrkesDir();
    await this.registerWorkflows(join(root, 'workflows'));
    await this.registerEventHandlers(join(root, 'event_handlers'));
  }

  private async registerWorkflows(dir: string): Promise<void> {
    const files = await this.listJson(dir);
    if (files.length === 0) {
      this.logger.log(`No workflow JSON under ${dir}`);
      return;
    }
    const metadata = this.clients!.getMetadataClient();
    for (const path of files) {
      try {
        const def = JSON.parse(await fs.readFile(path, 'utf8')) as WorkflowDef;
        await metadata.registerWorkflowDef(def, true);
        this.logger.log(`Registered workflow ${def.name}@v${def.version}`);
      } catch (err) {
        this.logger.error(
          `Failed to register workflow from ${path}: ${(err as Error).message}`,
        );
      }
    }
  }

  private async registerEventHandlers(dir: string): Promise<void> {
    const files = await this.listJson(dir);
    if (files.length === 0) {
      this.logger.log(`No event-handler JSON under ${dir}`);
      return;
    }
    const events = this.clients!.getEventClient();
    for (const path of files) {
      try {
        const handler = JSON.parse(await fs.readFile(path, 'utf8')) as EventHandler;
        await this.upsertEventHandler(events, handler);
        this.logger.log(
          `Registered event handler ${handler.name} -> ${handler.event}`,
        );
      } catch (err) {
        this.logger.error(
          `Failed to register event handler from ${path}: ${(err as Error).message}`,
        );
      }
    }
  }

  private async upsertEventHandler(
    events: ReturnType<OrkesClients['getEventClient']>,
    handler: EventHandler,
  ): Promise<void> {
    if (!handler.name) {
      throw new Error('EventHandler JSON must include a "name" field');
    }
    try {
      await events.getEventHandlerByName(handler.name);
      await events.updateEventHandler(handler);
    } catch {
      await events.addEventHandler(handler);
    }
  }

  private async listJson(dir: string): Promise<string[]> {
    try {
      const entries = await fs.readdir(dir);
      return entries
        .filter((f) => f.endsWith('.json'))
        .map((f) => join(dir, f))
        .sort();
    } catch {
      return [];
    }
  }

  private async resolveOrkesDir(): Promise<string> {
    const candidates = [
      join(process.cwd(), 'orkes'),
      join(__dirname, '..', '..', 'orkes'),
      join(__dirname, '..', '..', '..', 'orkes'),
    ];
    for (const c of candidates) {
      try {
        await fs.access(c);
        return c;
      } catch {
        // try next
      }
    }
    return candidates[0];
  }
}
