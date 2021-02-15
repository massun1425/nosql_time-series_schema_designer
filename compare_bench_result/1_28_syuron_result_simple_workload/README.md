# README

items.name と users.firstname では items.name の cardinality が大幅に小さく，最終時刻向けに最適化する設定で users.firstname にジョインプランをだしているが，items.name に MV プランを出しっぱなしの設定が一番性能がよくなってしまった．
もう少し，items.name と cardinality の近い users.lastname を使って実験をする．
頻度変化は 99.9 対 0.1 を使った．
ストレージサイズは static が  1225200000 だったので，その 99% の --max_space 1212948000 で実験
また，rubis に各属性の cardinality を記載したバージョンを作成した．
