## here we start the Zomato REal time project



## Step 1: Registering the users actions in the platform from the Web APP
## Steps create a Fastapi app to create endpoint to register user actions and push them to Kafka topic
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app=FastAPI()

origins=[
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True, 
    allow_methods=["*"],
    allow_headers=["*"],
)


class UserAction(BaseModel):
    user_id: int 
    action: str
    item_id: int


@app.post("/orders",response_model=UserAction)
async def create_order(action:UserAction):
    ## here will push the data to kafka topics

    pass




## Next Step is to create the kafka producer to push the data to kafka topics but it will be done by events_gateway
if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=8000)


