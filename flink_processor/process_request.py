################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from messages_pb2 import TransferRequest, TransferResponse, AmountTransfered

from statefun import StatefulFunctions
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

functions = StatefulFunctions()


@functions.bind("demo/transfer/request")
def check_request(context, transfer_request: TransferRequest):
    state = context.state('amount').unpack(AmountTransfered)
    if not state:
        state = AmountTransfered()
        state.amount = transfer_request.amount
    else:
        state.amount += transfer_request.amount
    context.state('amount').pack(state)

    response = send_acknowledgement(
        transfer_request.number, transfer_request.amount, state.amount)

    egress_message = kafka_egress_record(
        topic="transfer_response", key=transfer_request.number, value=response)
    context.pack_and_send_egress(
        "demo/transfer/acknowledgement", egress_message)


def send_acknowledgement(number, amount, total_amount):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["{} transfer successfully, Total amount transfered: {}",
                 "your transcaction limit exceeded"]
    # if amount < 100000:
    acknowledgement = templates[0].format(amount, total_amount)
    # else:
    # acknowledgement = templates[1]

    response = TransferResponse()
    response.number = number
    response.acknowledgement = acknowledgement

    return response


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/statefun', methods=['POST'])
def handle():
    print(request.data)
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run(debug=True)
