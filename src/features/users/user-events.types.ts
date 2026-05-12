export interface UserCreatedEvent {
  eventId: string;
  occurredAt: number;
  userId: string;
  email: string;
  displayName: string | null;
}

export const USER_CREATED_TOPIC = 'one-bth-dev-user-created-in-private';
