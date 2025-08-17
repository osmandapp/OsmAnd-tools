const brandsData = {
    '1': { name: 'Coca-Cola', owner: 'usa', ownerCount: 4800, globalCount: 5000, ownerPercent: 96, includeReason: 'Globally recognized', include: true },
    '2': { name: 'Toyota', owner: 'japan', ownerCount: 4300, globalCount: 4500, ownerPercent: 95, includeReason: 'Top automaker', include: true },
    '3': { name: 'Samsung', owner: 'south_korea', ownerCount: 3900, globalCount: 4000, ownerPercent: 97, includeReason: 'Electronics giant', include: true },
    '4': { name: 'Mercedes', owner: 'germany', ownerCount: 3200, globalCount: 3500, ownerPercent: 91, includeReason: 'Luxury vehicle leader', include: true },
    '5': { name: 'BMW', owner: 'germany', ownerCount: 2700, globalCount: 2800, ownerPercent: 96, includeReason: 'High regional presence', include: true },
    '6': { name: 'LVMH', owner: 'france', ownerCount: 1000, globalCount: 1100, ownerPercent: 90, includeReason: 'Luxury market dominance', include: true },
    '7': { name: 'IKEA', owner: 'sweden', ownerCount: 600, globalCount: 700, ownerPercent: 85, includeReason: 'Strong brand recognition', include: true },
    '8': { name: 'Zara', owner: 'spain', ownerCount: 550, globalCount: 650, ownerPercent: 84, includeReason: 'Fast fashion leader', include: true },
    '9': { name: 'SAP', owner: 'germany', ownerCount: 1400, globalCount: 1500, ownerPercent: 93, includeReason: 'Top brand', include: true },
    '10': { name: 'A very long German brand name that needs to be truncated', owner: 'germany', ownerCount: 50, globalCount: 50, ownerPercent: 100, includeReason: null, include: false },
    '11': { name: 'Volkswagen', owner: 'germany', ownerCount: 2000, globalCount: 3000, ownerPercent: 66, includeReason: null, include: false },
    '12': { name: 'Roshen', owner: 'ukraine', ownerCount: 500, globalCount: 600, ownerPercent: 83, includeReason: 'Top brand', include: true },
    '13': { name: 'MHP', owner: 'ukraine', ownerCount: 300, globalCount: 350, ownerPercent: 85, includeReason: 'Top brand', include: true },
    '14': { name: 'Apple', owner: 'usa', ownerCount: 4900, globalCount: 5100, ownerPercent: 96, includeReason: 'Top brand', include: true },
    '15': { name: 'Microsoft', owner: 'usa', ownerCount: 4500, globalCount: 4600, ownerPercent: 97, includeReason: 'Top brand', include: true },
    '16': { name: 'Shopify', owner: 'canada', ownerCount: 400, globalCount: 450, ownerPercent: 88, includeReason: 'Top brand', include: true },
    '17': { name: 'Ford', owner: 'usa', ownerCount: 1500, globalCount: 2000, ownerPercent: 75, includeReason: null, include: false },
    '18': { name: 'Tencent', owner: 'china', ownerCount: 3000, globalCount: 3100, ownerPercent: 96, includeReason: 'Top brand', include: true },
    '19': { name: 'Sony', owner: 'japan', ownerCount: 2000, globalCount: 2200, ownerPercent: 90, includeReason: null, include: false },
    '20': { name: 'Nintendo', owner: 'japan', ownerCount: 1800, globalCount: 1900, ownerPercent: 94, includeReason: null, include: false }
};

const brandsRegionData = {
    'planet': { parent: null, included: '10', depth: 0, leaf: false, brands: [ { id: 1, count: 5000 }, { id: 2, count: 4500 }, { id: 3, count: 4000 }, { id: 4, count: 3500 } ] },
    'europe': { parent: 'planet', included: '5', depth: 1, leaf: false, brands: [ { id: 4, count: 1500 }, { id: 5, count: 1200 }, { id: 6, count: 800 }, { id: 1, count: 900 }, { id: 7, count: 600 }, { id: 8, count: 550 } ] },
    'germany': { parent: 'europe', included: '3', depth: 2, leaf: true, brands: [ { id: 4, count: 1200 }, { id: 5, count: 1000 }, { id: 9, count: 800 }, { id: 10, count: 50 }, { id: 11, count: 5 } ] },
    'ukraine': { parent: 'europe', included: '2', depth: 2, leaf: true, brands: [ { id: 12, count: 500 }, { id: 13, count: 300 }, { id: 1, count: 150 }, { id: 4, count: 50 } ] },
    'north_america': { parent: 'planet', included: '4', depth: 1, leaf: false, brands: [ { id: 1, count: 2500 }, { id: 14, count: 2200 }, { id: 15, count: 1800 }, { id: 2, count: 800 }, { id: 16, count: 400 } ] },
    'usa': { parent: 'north_america', included: '3', depth: 2, leaf: true, brands: [ { id: 1, count: 2400 }, { id: 14, count: 2100 }, { id: 15, count: 1750 }, { id: 17, count: 6 } ] },
    'asia': { parent: 'planet', included: '3', depth: 1, leaf: false, brands: [ { id: 2, count: 2000 }, { id: 3, count: 1800 }, { id: 18, count: 1500 }, { id: 4, count: 400 } ] },
    'japan': { parent: 'asia', included: '1', depth: 2, leaf: true, brands: [ { id: 2, count: 1900 }, { id: 19, count: 8 }, { id: 20, count: 4 } ] }
};
