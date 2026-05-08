// 시장 탭 + 종목 검색 — 클라이언트 사이드 필터.
(function () {
  const search = document.getElementById('search');
  const tabs   = document.querySelectorAll('.filter-bar .tab');
  const blocks = document.querySelectorAll('.market-block');
  if (!search || !blocks.length) return;

  let activeTab = 'all';

  function update() {
    const term = (search.value || '').trim().toLowerCase();
    blocks.forEach(b => {
      const market = b.dataset.market;
      const tabMatch = activeTab === 'all' || activeTab === market;
      b.style.display = tabMatch ? '' : 'none';
      if (!tabMatch) return;

      let visible = 0;
      b.querySelectorAll('.ticker-card').forEach(c => {
        const text = (c.dataset.search || '').toLowerCase();
        const match = !term || text.includes(term);
        c.style.display = match ? '' : 'none';
        if (match) visible++;
      });
      const empty = b.querySelector('.search-empty');
      if (empty) empty.style.display = (visible === 0) ? '' : 'none';
    });
  }

  tabs.forEach(t => {
    t.addEventListener('click', () => {
      tabs.forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      activeTab = t.dataset.market;
      update();
    });
  });

  search.addEventListener('input', update);

  // '/'  키로 검색창 포커스 (텔레그램 안 통하던 키보드 친구 배려)
  document.addEventListener('keydown', e => {
    if (e.key === '/' && document.activeElement !== search) {
      e.preventDefault();
      search.focus();
    }
  });
})();
