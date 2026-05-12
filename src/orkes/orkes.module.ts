import { Logger, Module } from '@nestjs/common';
import { OrkesClients } from '@io-orkes/conductor-javascript';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/app-config.service';
import { ORKES_CLIENTS } from './orkes.tokens';
import { OrkesBootstrapService } from './orkes-bootstrap.service';
import { OrkesController } from './orkes.controller';

@Module({
  imports: [AppConfigModule],
  controllers: [OrkesController],
  providers: [
    {
      provide: ORKES_CLIENTS,
      inject: [AppConfigService],
      useFactory: async (
        cfg: AppConfigService,
      ): Promise<OrkesClients | null> => {
        const log = new Logger('OrkesModule');
        const { enabled, serverUrl, key, secret } = cfg.orkes;
        if (!enabled) {
          log.log('Orkes disabled (ORKES_ENABLED=false)');
          return null;
        }
        if (!serverUrl || !key || !secret) {
          log.warn(
            'Orkes enabled but ORKES_SERVER_URL/ORKES_KEY/ORKES_SECRET are missing; client will not be created',
          );
          return null;
        }
        try {
          return await OrkesClients.from({
            keyId: key,
            keySecret: secret,
            serverUrl,
          });
        } catch (err) {
          log.error(
            `Failed to construct OrkesClients: ${(err as Error).message}`,
          );
          return null;
        }
      },
    },
    OrkesBootstrapService,
  ],
  exports: [ORKES_CLIENTS],
})
export class OrkesModule {}
