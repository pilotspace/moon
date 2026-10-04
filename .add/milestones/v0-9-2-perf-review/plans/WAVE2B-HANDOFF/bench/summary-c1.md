## /tmp/claude-0/-home-user-moon/d1b785a6-84fa-5659-9361-52c93e4ab21f/scratchpad/bench2b/results-monoio.csv  (load1 during runs 0.78-2.35)
| cell | n | base (w2a) | 2b | delta | paired 2b/base median [min..max] | verdict |
|---|---|---|---|---|---|---|
| s1-aofno-everysec-p1-c50 get | 7 | 111.9K (104.2K–116.1K) | 111.4K (101.8K–115.9K) | -0.4% | -2.3% [-11..+7] | inside spread |
| s1-aofno-everysec-p1-c50 set | 7 | 111.7K (108.2K–124.5K) | 116.1K (109.3K–119.5K) | +4.0% | -1.4% [-8..+8] | inside spread |
| s1-aofno-everysec-p16-c50 set | 7 | 1.10M (956.9K–1.25M) | 1.09M (766.3K–1.15M) | -0.5% | -2.7% [-26..+2] | inside spread |
| s1-aofyes-everysec-p1-c1 set | 7 | 23.0K (22.7K–23.7K) | 22.8K (21.7K–23.1K) | -1.1% | -3.7% [-6..+1] | inside spread |
| s1-aofyes-everysec-p1-c50 set | 7 | 110.4K (106.3K–118.1K) | 116.4K (108.6K–123.7K) | +5.5% | +5.8% [-1..+9] | inside spread |
| s1-aofyes-everysec-p16-c1 set | 7 | 271.7K (241.3K–293.3K) | 267.7K (242.7K–275.1K) | -1.5% | -0.4% [-8..+2] | inside spread |
| s1-aofyes-everysec-p16-c50 set | 7 | 913.2K (803.2K–980.4K) | 885.0K (729.9K–952.4K) | -3.1% | -3.1% [-19..+6] | inside spread |
| s4-aofyes-everysec-p1-c50 set | 7 | 60.8K (56.7K–65.0K) | 63.8K (61.8K–66.0K) | +5.0% | +6.9% [-1..+9] | inside spread |
| s4-aofyes-everysec-p16-c50 set | 7 | 729.9K (694.4K–760.5K) | 724.6K (694.4K–751.9K) | -0.7% | +1.9% [-8..+7] | inside spread |
| s1-aofyes-always-p16-c50 set | 7 | 489.0K (421.9K–523.6K) | 480.8K (401.6K–516.8K) | -1.7% | -2.9% [-16..+22] | inside spread |
## /tmp/claude-0/-home-user-moon/d1b785a6-84fa-5659-9361-52c93e4ab21f/scratchpad/bench2b/results-tokio.csv  (load1 during runs 1.89-3.23)
| cell | n | base (w2a) | 2b | delta | paired 2b/base median [min..max] | verdict |
|---|---|---|---|---|---|---|
| s1-aofno-everysec-p1-c50 get | 7 | 112.7K (103.7K–123.3K) | 113.1K (109.7K–127.1K) | +0.3% | +3.0% [-7..+7] | inside spread |
| s1-aofno-everysec-p1-c50 set | 7 | 106.5K (97.3K–129.0K) | 115.7K (101.4K–125.3K) | +8.6% | +4.3% [-7..+13] | inside spread (4/7 pairs) |
| s1-aofno-everysec-p16-c50 set | 7 | 410.7K (354.6K–469.5K) | 407.3K (360.4K–452.5K) | -0.8% | -0.8% [-6..+10] | inside spread |
| s1-aofyes-everysec-p1-c1 set | 7 | 18.0K (16.2K–18.5K) | 21.2K (17.6K–21.9K) | +18.2% | +18.0% [+7..+21] | **win** (7/7 pairs) |
| s1-aofyes-everysec-p1-c50 set | 7 | 94.4K (85.3K–101.0K) | 113.6K (99.4K–119.8K) | +20.4% | +18.5% [-2..+34] | probable win (6/7 pairs) |
| s1-aofyes-everysec-p16-c1 set | 7 | 177.5K (167.4K–186.7K) | 200.2K (189.0K–207.5K) | +12.8% | +10.0% [+7..+24] | **win** (ranges disjoint) |
| s1-aofyes-everysec-p16-c50 set | 7 | 301.7K (276.6K–307.7K) | 497.5K (387.6K–539.1K) | +64.9% | +65.7% [+40..+75] | **win** (ranges disjoint) |
| s4-aofyes-everysec-p1-c50 set | 7 | 65.2K (58.5K–71.8K) | 65.3K (61.3K–68.8K) | +0.1% | +0.1% [-6..+7] | inside spread |
| s4-aofyes-everysec-p16-c50 set | 7 | 492.6K (468.4K–546.4K) | 653.6K (581.4K–706.7K) | +32.7% | +28.3% [+20..+45] | **win** (ranges disjoint) |
| s1-aofyes-always-p16-c50 set | 7 | 414.9K (379.5K–470.6K) | 404.9K (383.9K–483.1K) | -2.4% | -3.0% [-13..+7] | inside spread |
