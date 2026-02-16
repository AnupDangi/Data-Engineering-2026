/**
 * Event tracking utility for FlowGuard
 * Sends events to Events Gateway (port 8000)
 */

const EVENTS_GATEWAY_URL = 'http://localhost:8000';

// Generate session-based user ID
export function getUserId(): string {
  // Check if running in browser (not SSR)
  if (typeof window === 'undefined') {
    return 'user_ssr';
  }
  
  let userId = localStorage.getItem('flowguard_user_id');
  if (!userId) {
    userId = `user_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    localStorage.setItem('flowguard_user_id', userId);
  }
  return userId;
}

// Get numeric user ID (hash of string ID)
function getNumericUserId(): number {
  const userId = getUserId();
  // Simple hash to convert string to number
  let hash = 0;
  for (let i = 0; i < userId.length; i++) {
    hash = ((hash << 5) - hash) + userId.charCodeAt(i);
    hash |= 0; // Convert to 32bit integer
  }
  return Math.abs(hash);
}

// Send order event - returns orderId for navigation
export async function trackOrderEvent(
  itemId: number, 
  itemName: string, 
  price: number,
  itemCategory: string = '',
  itemDescription: string = '',
  itemImageUrl: string = ''
): Promise<{ orderId: string; success: boolean }> {
  try {
    // Server generates order_id and event_id - no client-side generation
    const payload = {
      user_id: getNumericUserId(),
      item_id: itemId,
      item_name: itemName,
      price: price,
      timestamp: new Date().toISOString(),
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/orders/`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error('Failed to track order:', await response.text());
      return { orderId: '', success: false };
    }

    const result = await response.json();
    const orderId = result.order_id;  // Server-generated UUID
    
    console.log('✅ Order placed:', result);
    
    // Store order details in localStorage for order page
    if (typeof window !== 'undefined') {
      const orderDetails = {
        order_id: orderId,
        food_id: itemId,
        name: itemName,
        category: itemCategory,
        price: price,
        description: itemDescription,
        image_url: itemImageUrl,
        user_id: getUserId(),
        timestamp: payload.timestamp
      };
      localStorage.setItem(`order_${orderId}`, JSON.stringify(orderDetails));
    }
    
    return { orderId, success: true };
  } catch (error) {
    console.error('Error tracking order:', error);
    return { orderId: '', success: false };
  }
}

// Send click event
export async function trackClickEvent(adId: string, isClick: boolean): Promise<boolean> {
  try {
    // Extract food_id from adId like "food_5" -> 5
    const itemId = adId.startsWith('food_') ? parseInt(adId.split('_')[1]) : null;
    
    const payload = {
      user_id: getNumericUserId(),
      ad_id: adId,
      item_id: itemId,  // ✅ Now includes actual food item ID
      session_id: getUserId(),  // ✅ Use user session ID
      is_click: isClick,
      event_type: isClick ? 'click' : 'impression',
      timestamp: new Date().toISOString(),
    };

    const response = await fetch(`${EVENTS_GATEWAY_URL}/api/v1/clicks/`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error('Failed to track click:', await response.text());
      return false;
    }

    console.log('✅ Click event tracked:', payload);
    return true;
  } catch (error) {
    console.error('Error tracking click:', error);
    return false;
  }
}
