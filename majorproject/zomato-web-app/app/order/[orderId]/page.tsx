'use client';

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";

interface OrderDetails {
  order_id: string;
  food_id: number;
  name: string;
  category: string;
  price: number;
  description: string;
  image_url: string;
  user_id: string;
  timestamp: string;
}

export default function OrderPage() {
  const params = useParams();
  const router = useRouter();
  const [order, setOrder] = useState<OrderDetails | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const orderId = params.orderId as string;
    
    // Parse order details from URL or localStorage
    const orderData = localStorage.getItem(`order_${orderId}`);
    
    if (orderData) {
      setOrder(JSON.parse(orderData));
    } else {
      // If no order data, redirect to home
      setTimeout(() => router.push('/'), 2000);
    }
    
    setLoading(false);
  }, [params.orderId, router]);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-4 border-red-600 border-t-transparent"></div>
      </div>
    );
  }

  if (!order) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-900 mb-4">Order Not Found</h1>
          <p className="text-gray-600 mb-6">Redirecting to home...</p>
          <Link href="/" className="text-red-600 hover:underline">Go to Home</Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-4 flex justify-between items-center">
          <Link href="/" className="flex items-center gap-2 text-gray-600 hover:text-gray-900">
            <span className="text-2xl">←</span>
            <span className="font-semibold">Back to Menu</span>
          </Link>
          <h1 className="text-2xl font-bold text-red-600">🍕 FlowGuard Food</h1>
        </div>
      </header>

      {/* Success Message */}
      <div className="max-w-4xl mx-auto px-4 py-8">
        <div className="bg-green-50 border-2 border-green-200 rounded-xl p-6 mb-8">
          <div className="flex items-center gap-3 mb-2">
            <span className="text-4xl">✅</span>
            <h2 className="text-2xl font-bold text-green-800">Order Placed Successfully!</h2>
          </div>
          <p className="text-green-700 ml-14">Your order has been received and is being processed.</p>
        </div>

        {/* Order Details Card */}
        <div className="bg-white rounded-xl shadow-lg overflow-hidden">
          <div className="bg-red-600 text-white px-6 py-4">
            <h3 className="text-xl font-bold">Order Details</h3>
            <p className="text-red-100 text-sm">Order ID: {order.order_id}</p>
          </div>

          <div className="p-6">
            <div className="grid md:grid-cols-2 gap-6">
              {/* Food Image */}
              <div className="relative h-64 md:h-80 bg-gray-100 rounded-lg overflow-hidden">
                <img
                  src={order.image_url}
                  alt={order.name}
                  className="w-full h-full object-cover"
                />
              </div>

              {/* Food Details */}
              <div className="flex flex-col">
                <div className="mb-4">
                  <span className="inline-block bg-gray-100 text-gray-700 text-xs px-3 py-1 rounded-full mb-2">
                    {order.category}
                  </span>
                  <h2 className="text-3xl font-bold text-gray-900 mb-2">{order.name}</h2>
                  <p className="text-gray-600 text-lg leading-relaxed">{order.description}</p>
                </div>

                <div className="mt-auto space-y-4">
                  {/* Price */}
                  <div className="flex items-center justify-between py-3 border-t border-gray-200">
                    <span className="text-gray-600 font-medium">Price</span>
                    <span className="text-3xl font-bold text-red-600">₹{order.price}</span>
                  </div>

                  {/* Order Info */}
                  <div className="bg-gray-50 rounded-lg p-4 space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Order Time</span>
                      <span className="font-medium text-gray-900">
                        {new Date(order.timestamp).toLocaleString('en-IN', {
                          dateStyle: 'medium',
                          timeStyle: 'short'
                        })}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">User ID</span>
                      <span className="font-mono text-xs text-gray-900">{order.user_id.slice(-12)}</span>
                    </div>
                  </div>

                  {/* Estimated Delivery */}
                  <div className="bg-green-50 rounded-lg p-4 text-center">
                    <p className="text-green-800 font-semibold text-lg">Estimated Delivery</p>
                    <p className="text-green-600 text-2xl font-bold">30-45 minutes</p>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="bg-gray-50 px-6 py-4 flex gap-4">
            <Link 
              href="/"
              className="flex-1 bg-red-600 text-white py-3 rounded-lg font-semibold hover:bg-red-700 transition text-center"
            >
              Order More Items
            </Link>
            <button
              onClick={() => alert('Track Order feature coming soon!')}
              className="flex-1 bg-gray-200 text-gray-700 py-3 rounded-lg font-semibold hover:bg-gray-300 transition"
            >
              Track Order
            </button>
          </div>
        </div>

        {/* Event Tracking Info */}
        <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-4">
          <p className="text-blue-800 text-sm">
            <span className="font-semibold">📊 Event Tracked:</span> This order event has been sent to Kafka 
            (topic: raw.orders.v1) for real-time processing and analytics.
          </p>
        </div>
      </div>
    </div>
  );
}
