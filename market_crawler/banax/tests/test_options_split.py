async def test_options_split():
    options_price2_soldout_list = [
        "KAIGEN Z 150S \xa0￦650,000 재고있음",
        "KAIGEN Z 150SL \xa0￦650,000 재고있음",
    ]

    for idx, options in enumerate(options_price2_soldout_list):
        option, price, soldout = get_option_price_and_soldout(options)

        if idx == 0:
            assert option == "KAIGEN Z 150S"
            assert price == "650000"
            assert soldout == "재고있음"
        elif idx == 1:
            assert option == "KAIGEN Z 150SL"
            assert price == "650000"
            assert soldout == "재고있음"

    options_price2_soldout_list = [
        "FX 군도 1-450 \xa0￦88,000 재고없음",
        "FX 군도 1-500 \xa0￦90,000 재고없음",
        "FX 군도 1-530 \xa0￦92,000 재고있음",
        "FX 군도 1.5-530 \xa0￦99,000 재고있음",
        "FX 군도 2-450 \xa0￦94,000 재고있음",
        "FX 군도 2-500 \xa0￦105,000 재고없음",
        "FX 군도 2-530 \xa0￦107,000 재고있음",
        "FX 군도 3-450 \xa0￦98,000 재고없음",
        "FX 군도 3-530 \xa0￦112,000 재고없음",
    ]

    for idx, options in enumerate(options_price2_soldout_list):
        option, price, soldout = get_option_price_and_soldout(options)

        if idx == 0:
            assert option == "FX 군도 1-450"
            assert price == "88000"
            assert soldout == "재고없음"
        elif idx == 1:
            assert option == "FX 군도 1-500"
            assert price == "90000"
            assert soldout == "재고없음"
        elif idx == 2:
            assert option == "FX 군도 1-530"
            assert price == "92000"
            assert soldout == "재고있음"
        elif idx == 3:
            assert option == "FX 군도 1.5-530"
            assert price == "99000"
            assert soldout == "재고있음"
        elif idx == 4:
            assert option == "FX 군도 2-450"
            assert price == "94000"
            assert soldout == "재고있음"
        elif idx == 5:
            assert option == "FX 군도 2-500"
            assert price == "105000"
            assert soldout == "재고없음"
        elif idx == 6:
            assert option == "FX 군도 2-530"
            assert price == "107000"
            assert soldout == "재고있음"
        elif idx == 7:
            assert option == "FX 군도 3-450"
            assert price == "98000"
            assert soldout == "재고없음"
        elif idx == 8:
            assert option == "FX 군도 3-530"
            assert price == "112000"
            assert soldout == "재고없음"


def get_option_price_and_soldout(options: str):
    splitted_options = options.split(" \xa0")
    option = splitted_options[0]
    # ? Remaining text after model name, if greater than 1 then it means it contains both price and soldout text or that there were more than one " \xa0"
    if len(splitted_options[1:]) > 1:
        price = splitted_options[1:2]
        soldout = splitted_options[2:]
    elif len(splitted_options[1:]) == 1:
        price = "".join(splitted_options[1:]).split(" ")[0]
        soldout = "".join(splitted_options[1:]).split(" ")[-1]
    else:
        raise ValueError("Price is not present")

    price = "".join(filter(lambda x: x.isdigit(), price))
    return option, price, soldout
