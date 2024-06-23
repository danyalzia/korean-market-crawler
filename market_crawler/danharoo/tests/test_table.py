# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.danharoo.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://danharoo.com/product/detail.html?product_no=5051&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51886&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50865&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52443&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47866&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49103&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47826&cate_no=352&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=16819&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39132&cate_no=340&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50254&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43485&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=7769&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=17854&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=3061&cate_no=389&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44644&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35507&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53377&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37452&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43365&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=21106&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49958&cate_no=352&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47779&cate_no=375&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43884&cate_no=165&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51601&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29139&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46298&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=17337&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49932&cate_no=334&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50781&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46844&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30546&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53422&cate_no=457&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30245&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41352&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38544&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30693&cate_no=361&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35148&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=9715&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=5118&cate_no=177&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42570&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51988&cate_no=893&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=25125&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=12239&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39038&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39924&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37937&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42251&cate_no=893&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42128&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49260&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49626&cate_no=373&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53102&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28243&cate_no=383&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=12337&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48341&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=16822&cate_no=458&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47977&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=34565&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49258&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=24037&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=32796&cate_no=361&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35448&cate_no=386&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=14105&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43375&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51888&cate_no=363&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39214&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52943&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=8940&cate_no=332&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30157&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49380&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=4912&cate_no=330&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=10378&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47955&cate_no=354&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44540&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=5354&cate_no=334&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=21008&cate_no=363&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52817&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=20615&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37165&cate_no=354&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42343&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48879&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41021&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51387&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41411&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46884&cate_no=353&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=23647&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51655&cate_no=379&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30844&cate_no=357&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41790&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50428&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53028&cate_no=892&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37590&cate_no=334&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48149&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42190&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=33268&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48920&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50907&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45805&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35061&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46145&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48273&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45791&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44170&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=32276&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45499&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50269&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50397&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49551&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=24584&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51360&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49339&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49718&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=19888&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42607&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47512&cate_no=352&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42805&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=17176&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51644&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35640&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=10585&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40622&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52204&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38052&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=20259&cate_no=188&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37096&cate_no=353&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45181&cate_no=376&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46139&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39293&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30439&cate_no=361&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=33848&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=34436&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31532&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44794&cate_no=165&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=36423&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=14630&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46440&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=2441&cate_no=237&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=12478&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42442&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=11491&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53071&cate_no=368&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=15018&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=22872&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41994&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29166&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49600&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48449&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52065&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40379&cate_no=383&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40750&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=36560&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53499&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=20087&cate_no=375&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49155&cate_no=384&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51065&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48977&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31554&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=15250&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41443&cate_no=379&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=22786&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40422&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51037&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49349&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41322&cate_no=336&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37606&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40157&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41748&cate_no=386&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=26818&cate_no=358&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46539&cate_no=356&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=27208&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44246&cate_no=893&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=17051&cate_no=458&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=13488&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=26229&cate_no=382&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=24651&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=21711&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50397&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38158&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52004&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50384&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42892&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=20504&cate_no=383&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37986&cate_no=165&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43219&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50579&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28942&cate_no=355&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45672&cate_no=356&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37987&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41644&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29786&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37870&cate_no=353&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29755&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28818&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53353&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38528&cate_no=354&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47124&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=24929&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=25781&cate_no=355&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41401&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49693&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=13491&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38402&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37319&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=18842&cate_no=342&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40434&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53074&cate_no=166&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40561&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31653&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50888&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48593&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30515&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=7198&cate_no=363&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44856&cate_no=336&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49787&cate_no=386&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51851&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29787&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28489&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=36795&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30561&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50842&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52500&cate_no=894&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52359&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=6794&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44027&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=44694&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39615&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50042&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=24031&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=53560&cate_no=363&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50379&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50470&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=27418&cate_no=334&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51821&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=29023&cate_no=375&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39682&cate_no=353&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50972&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37578&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42560&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40019&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49557&cate_no=346&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48020&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30765&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45216&cate_no=352&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37109&cate_no=382&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51291&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43198&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50036&cate_no=384&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=27797&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28790&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48761&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41084&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40258&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48194&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=12356&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51967&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=14862&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43061&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=34622&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40793&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46579&cate_no=360&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=16420&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=15838&cate_no=356&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=46579&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49132&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=32399&cate_no=383&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41118&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=16002&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41897&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51280&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=25354&cate_no=355&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41605&cate_no=348&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28503&cate_no=383&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52303&cate_no=369&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31348&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31998&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=37964&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=5410&cate_no=362&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47501&cate_no=165&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45640&cate_no=251&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=49801&cate_no=347&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47253&cate_no=386&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=35396&cate_no=1083&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50265&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31652&cate_no=236&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30534&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28949&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=23428&cate_no=167&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41870&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41840&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43262&cate_no=334&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=38039&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40775&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28811&cate_no=371&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=34915&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50951&cate_no=340&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40114&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42669&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51998&cate_no=350&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=39698&cate_no=382&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50717&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47608&cate_no=338&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40035&cate_no=333&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=15162&cate_no=367&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52465&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51477&cate_no=356&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=26032&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=40865&cate_no=250&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=48095&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=28654&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43739&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47373&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41176&cate_no=345&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=13557&cate_no=368&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52657&cate_no=349&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=47891&cate_no=356&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=43407&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=17528&cate_no=359&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=45319&cate_no=386&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=52475&cate_no=372&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=25951&cate_no=385&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42575&cate_no=380&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=34670&cate_no=259&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51948&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=42012&cate_no=891&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=11256&cate_no=337&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=50294&cate_no=164&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=10277&cate_no=333&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=31352&cate_no=374&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=41304&cate_no=1084&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=51544&cate_no=893&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=30805&cate_no=234&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=3482&cate_no=345&display_group=1",
    }

    urls = list(urls)
    shuffle(urls)
    tasks = (extract(url, browser) for url in urls[:10])
    await asyncio.gather(*tasks)

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).unwrap()

    print(f"{table = }")

    await page.close()
