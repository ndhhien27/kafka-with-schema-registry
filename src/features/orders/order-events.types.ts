export interface OrderItem {
  sku: string;
  quantity: number;
}

export interface OrderPlacedEvent {
  eventId: string;
  occurredAt: number;
  orderId: string;
  userId: string;
  amountCents: number;
  currency: string;
  items: OrderItem[];
}

export const ORDER_PLACED_TOPIC = 'one-bth-dev-order-placed-in-private';
