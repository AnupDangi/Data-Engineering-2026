'use client';

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { trackOrderEvent, trackClickEvent, getUserId } from "@/lib/eventTracker";

interface FoodItem {
  food_id: number;
  name: string;
  category: string;
  price: number;
  description?: string;
  image_url: string;
  is_available: boolean;
}

export default function Home() {
  const router = useRouter();
  const [foods, setFoods] = useState<FoodItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);
  const [categories, setCategories] = useState<string[]>([]);
  const [userId, setUserId] = useState<string>('');

  useEffect(() => {
    // Set user ID on client side only
    setUserId(getUserId());
    
    // Fetch categories
    fetch('http://localhost:8001/api/foods/categories/list')
      .then(res => res.json())
      .then(data => setCategories(data))
      .catch(err => console.error('Error fetching categories:', err));

    // Fetch foods
    fetchFoods();
  }, []);

  const fetchFoods = (category?: string) => {
    setLoading(true);
    const url = category 
      ? `http://localhost:8001/api/foods?category=${category}`
      : 'http://localhost:8001/api/foods';
    
    fetch(url)
      .then(res => res.json())
      .then(data => {
        setFoods(data.items);
        setLoading(false);
      })
      .catch(err => {
        console.error('Error fetching foods:', err);
        setLoading(false);
      });
  };

  const handleCategoryClick = (category: string | null) => {
    setSelectedCategory(category);
    fetchFoods(category || undefined);
  };

  const handleOrderClick = async (food: FoodItem, e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent card click from firing
    
    // Send order event to Events Gateway and navigate to order page
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

  const handleFoodImpression = (food: FoodItem) => {
    // Track food item impression (view)
    trackClickEvent(`food_${food.food_id}`, false);
  };

  const handleFoodClick = (food: FoodItem) => {
    // Track food item click and navigate to detail page
    trackClickEvent(`food_${food.food_id}`, true);
    router.push(`/food/${food.food_id}`);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-4 flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-red-600">🍕 FlowGuard Food</h1>
            <p className="text-gray-600 text-sm">Delicious food, lightning-fast delivery</p>
          </div>
          <div className="text-right">
            <p className="text-xs text-gray-400">Session ID</p>
            <p className="text-xs font-mono text-gray-600">{userId ? userId.slice(-12) : '...'}</p>
          </div>
        </div>
      </header>

      {/* Category Filter */}
      <div className="bg-white border-b sticky top-[72px] z-10">
        <div className="max-w-7xl mx-auto px-4 py-3">
          <div className="flex gap-2 overflow-x-auto">
            <button
              onClick={() => handleCategoryClick(null)}
              className={`px-4 py-2 rounded-full text-sm font-medium whitespace-nowrap transition ${
                selectedCategory === null
                  ? 'bg-red-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              All
            </button>
            {categories.map(cat => (
              <button
                key={cat}
                onClick={() => handleCategoryClick(cat)}
                className={`px-4 py-2 rounded-full text-sm font-medium whitespace-nowrap transition ${
                  selectedCategory === cat
                    ? 'bg-red-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {cat}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Food Grid */}
      <main className="max-w-7xl mx-auto px-4 py-8">
        {loading ? (
          <div className="text-center py-12">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-red-600 border-t-transparent"></div>
            <p className="mt-4 text-gray-600">Loading delicious food...</p>
          </div>
        ) : (
          <>
            <div className="mb-6">
              <h2 className="text-2xl font-semibold text-gray-900">
                {selectedCategory || 'All Items'} 
                <span className="text-gray-500 text-lg ml-2">({foods.length})</span>
              </h2>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
              {foods.map(food => (
                <div 
                  key={food.food_id} 
                  className="bg-white rounded-xl shadow-sm hover:shadow-lg transition-all overflow-hidden group cursor-pointer"
                  onClick={() => handleFoodClick(food)}
                  onMouseEnter={() => handleFoodImpression(food)}
                >
                  <div className="relative h-48 bg-gray-100">
                    <img
                      src={food.image_url}
                      alt={food.name}
                      className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                    />
                    {!food.is_available && (
                      <div className="absolute inset-0 bg-black bg-opacity-50 flex items-center justify-center">
                        <span className="bg-red-600 text-white px-4 py-2 rounded-full font-semibold">
                          Unavailable
                        </span>
                      </div>
                    )}
                  </div>
                  
                  <div className="p-4">
                    <div className="flex items-start justify-between mb-2">
                      <h3 className="font-semibold text-gray-900 text-lg">{food.name}</h3>
                    </div>
                    
                    <div className="flex items-center justify-between mb-3">
                      <span className="text-xs bg-gray-100 text-gray-700 px-2 py-1 rounded">
                        {food.category}
                      </span>
                      <span className="text-lg font-bold text-gray-900">
                        ₹{food.price}
                      </span>
                    </div>
                    
                    <button
                      onClick={(e) => handleOrderClick(food, e)}
                      disabled={!food.is_available}
                      className={`w-full py-2 rounded-lg font-semibold transition ${
                        food.is_available
                          ? 'bg-red-600 text-white hover:bg-red-700'
                          : 'bg-gray-200 text-gray-400 cursor-not-allowed'
                      }`}
                    >
                      {food.is_available ? 'Order Now' : 'Out of Stock'}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </main>

      {/* Footer */}
      <footer className="bg-white border-t mt-12">
        <div className="max-w-7xl mx-auto px-4 py-6 text-center text-gray-600 text-sm">
          <p>FlowGuard - Real time Data Tracking </p>
          <p className="text-xs text-gray-400 mt-1">
            PostgreSQL → Food Catalog API (port 8001) → Next.js UI
          </p>
        </div>
      </footer>
    </div>
  );
}
