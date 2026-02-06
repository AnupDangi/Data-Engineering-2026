'use client';

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { trackOrderEvent, getUserId } from "@/lib/eventTracker";

interface FoodItem {
  food_id: number;
  name: string;
  category: string;
  price: number;
  description?: string;
  image_url: string;
  is_available: boolean;
}

export default function FoodDetailPage() {
  const params = useParams();
  const router = useRouter();
  const [food, setFood] = useState<FoodItem | null>(null);
  const [loading, setLoading] = useState(true);
  const [userId, setUserId] = useState<string>('');

  useEffect(() => {
    setUserId(getUserId());
    
    const foodId = params.foodId as string;
    
    // Fetch food details from API
    fetch(`http://localhost:8001/api/foods/${foodId}`)
      .then(res => {
        if (!res.ok) throw new Error('Food not found');
        return res.json();
      })
      .then(data => {
        setFood(data);
        setLoading(false);
      })
      .catch(err => {
        console.error('Error fetching food:', err);
        setLoading(false);
      });
  }, [params.foodId]);

  const handleOrderClick = async () => {
    if (!food) return;
    
    const { orderId, success } = await trackOrderEvent(
      food.food_id, 
      food.name, 
      food.price,
      food.category,
      food.description || '',
      food.image_url
    );
    
    if (success) {
      router.push(`/order/${orderId}`);
    } else {
      alert('Failed to place order. Please try again.');
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-4 border-red-600 border-t-transparent"></div>
      </div>
    );
  }

  if (!food) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-gray-900 mb-4">Food Item Not Found</h1>
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
          <div className="text-right">
            <p className="text-xs text-gray-400">Session ID</p>
            <p className="text-xs font-mono text-gray-600">{userId ? userId.slice(-12) : '...'}</p>
          </div>
        </div>
      </header>

      {/* Food Detail */}
      <div className="max-w-6xl mx-auto px-4 py-8">
        <div className="bg-white rounded-xl shadow-lg overflow-hidden">
          <div className="grid md:grid-cols-2 gap-0">
            {/* Food Image */}
            <div className="relative h-96 md:h-auto bg-gray-100">
              <img
                src={food.image_url}
                alt={food.name}
                className="w-full h-full object-cover"
              />
              {!food.is_available && (
                <div className="absolute inset-0 bg-black bg-opacity-50 flex items-center justify-center">
                  <span className="bg-red-600 text-white px-6 py-3 rounded-full font-bold text-xl">
                    Currently Unavailable
                  </span>
                </div>
              )}
            </div>

            {/* Food Details */}
            <div className="p-8 flex flex-col">
              <div className="mb-4">
                <span className="inline-block bg-red-100 text-red-700 text-sm px-3 py-1 rounded-full mb-3">
                  {food.category}
                </span>
                <h1 className="text-4xl font-bold text-gray-900 mb-4">{food.name}</h1>
                <p className="text-gray-600 text-lg leading-relaxed mb-6">
                  {food.description || 'Delicious food item from our menu.'}
                </p>
              </div>

              <div className="mt-auto space-y-4">
                {/* Price */}
                <div className="flex items-center justify-between py-4 border-t border-b border-gray-200">
                  <span className="text-gray-600 text-lg font-medium">Price</span>
                  <span className="text-4xl font-bold text-red-600">₹{food.price}</span>
                </div>

                {/* Order Button */}
                <button
                  onClick={handleOrderClick}
                  disabled={!food.is_available}
                  className={`w-full py-4 rounded-lg font-bold text-lg transition ${
                    food.is_available
                      ? 'bg-red-600 text-white hover:bg-red-700 shadow-lg hover:shadow-xl'
                      : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                  }`}
                >
                  {food.is_available ? '🛒 Order Now' : 'Out of Stock'}
                </button>

                {food.is_available && (
                  <p className="text-center text-gray-500 text-sm">
                    ⚡ Delivery in 30-45 minutes
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Additional Info */}
        <div className="mt-6 grid md:grid-cols-3 gap-4">
          <div className="bg-white rounded-lg p-4 shadow-sm text-center">
            <span className="text-3xl mb-2 block">🚚</span>
            <h3 className="font-semibold text-gray-900">Fast Delivery</h3>
            <p className="text-gray-600 text-sm">30-45 mins</p>
          </div>
          <div className="bg-white rounded-lg p-4 shadow-sm text-center">
            <span className="text-3xl mb-2 block">🔥</span>
            <h3 className="font-semibold text-gray-900">Fresh & Hot</h3>
            <p className="text-gray-600 text-sm">Prepared on order</p>
          </div>
          <div className="bg-white rounded-lg p-4 shadow-sm text-center">
            <span className="text-3xl mb-2 block">✨</span>
            <h3 className="font-semibold text-gray-900">Quality Food</h3>
            <p className="text-gray-600 text-sm">Best ingredients</p>
          </div>
        </div>
      </div>
    </div>
  );
}
