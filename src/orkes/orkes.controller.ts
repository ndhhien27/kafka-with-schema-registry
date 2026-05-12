import {
  Body,
  Controller,
  Get,
  HttpCode,
  Inject,
  Logger,
  Param,
  Post,
  ServiceUnavailableException,
} from '@nestjs/common';
import type { OrkesClients } from '@io-orkes/conductor-javascript';
import { ORKES_CLIENTS } from './orkes.tokens';

interface TestWorkflowInput {
  userId?: string;
  amountCents?: number;
  currency?: string;
  sku?: string;
}

const DEMO_WORKFLOW_NAME = 'kafka_demo_workflow';
const DEMO_WORKFLOW_VERSION = 1;

@Controller('orkes')
export class OrkesController {
  private readonly logger = new Logger(OrkesController.name);

  constructor(
    @Inject(ORKES_CLIENTS) private readonly clients: OrkesClients | null,
  ) {}

  @Post('test-workflow')
  @HttpCode(202)
  async startTestWorkflow(@Body() body: TestWorkflowInput) {
    const wf = this.requireWorkflowClient();
    const input: Record<string, unknown> = {
      userId: body.userId ?? 'u-demo',
      amountCents: body.amountCents ?? 12_345,
      currency: body.currency ?? 'USD',
      sku: body.sku ?? 'SKU-DEMO',
    };

    const workflowId = await wf.startWorkflow({
      name: DEMO_WORKFLOW_NAME,
      version: DEMO_WORKFLOW_VERSION,
      input,
    });
    this.logger.log(
      `Started workflow=${DEMO_WORKFLOW_NAME}@v${DEMO_WORKFLOW_VERSION} id=${workflowId}`,
    );
    return { status: 'accepted', workflowId };
  }

  @Get('workflow/:id')
  async getWorkflow(@Param('id') id: string) {
    const wf = this.requireWorkflowClient();
    const status = await wf.getWorkflowStatus(id, true, true);
    return status;
  }

  private requireWorkflowClient() {
    if (!this.clients) {
      throw new ServiceUnavailableException(
        'Orkes is disabled or not configured (ORKES_ENABLED, ORKES_SERVER_URL, ORKES_KEY, ORKES_SECRET)',
      );
    }
    return this.clients.getWorkflowClient();
  }
}
