package net.osmand.util.translit.japanese;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class NumberCreator {
    private static final BigDecimal OKU = BigDecimal.valueOf(100000000L);
    
    private boolean inside = false;
    private int beg = -1;
    private BigDecimal val = BigDecimal.ZERO;
    private BigDecimal valCardinal = BigDecimal.ZERO;
    private BigDecimal lastCardinal = BigDecimal.valueOf(10);
    private BigDecimal base = BigDecimal.valueOf(10);
    private BigDecimal oku = BigDecimal.ZERO;
    
    public boolean inside() {
        return this.inside;
    }
    
    public void increment(final int index) {
        if (!this.inside) {
            return;
        }
        if (this.beg < 0) {
            this.beg = index;
        }
    }
    
    public void turnToDecimalState(final int index) {
        if (!this.inside) {
            this.inside = true;
        }
        this.increment(index);
        this.base = BigDecimal.valueOf(0.1);
    }
    
    public void attachCardinal(final int index, final BigDecimal cardinal) {
        this.inside = true;
        this.increment(index);
        
        val = (0 == this.val.compareTo(BigDecimal.ZERO))
                ? BigDecimal.ONE
                : this.val;
        
        if (this.lastCardinal.compareTo(cardinal) < 0) {
            this.valCardinal = (this.val.compareTo(BigDecimal.ZERO) == 0 && this.valCardinal.compareTo(BigDecimal.ZERO) != 0)
                    ? this.valCardinal.multiply(cardinal)
                    : cardinal.multiply(this.valCardinal.add(val));
        } else {
            this.valCardinal = this.valCardinal.add(cardinal.multiply(val));
        }
        
        if (NumberCreator.OKU.compareTo(this.valCardinal) <= 0) {
            this.oku = this.oku.add(this.valCardinal);
            this.valCardinal = BigDecimal.ZERO;
        }
        
        this.lastCardinal = cardinal;
        this.val = BigDecimal.ZERO;
    }
    
    public void attachNumber(final int index, final BigDecimal number) {
        this.inside = true;
        this.increment(index);
        if (this.base.compareTo(BigDecimal.ONE) < 0) {
            this.val = this.val.add(this.base.multiply(number));
            this.base = this.base.multiply(BigDecimal.valueOf(0.1));
        } else {
            this.val = number.add(this.base.multiply(this.val));
        }
    }
    
    public String value() {
        return (this.oku.add(this.val.add(this.valCardinal))).toString();
    }
    
    public static List<String> convertNumber(final String src) {
        List<String> val = new ArrayList<>();
        NumberCreator creator = new NumberCreator();
        
        for (int i = 0; i < src.length(); i++) {
            Character c = src.charAt(i);
            
            if (JapaneseMapper.isDelimiter(c)) {
                creator.increment(i);
                continue;
            }
            
            if (JapaneseMapper.isDecimalPoint(c)) {
                creator.turnToDecimalState(i);
                continue;
            }
            
            BigDecimal cardinal = JapaneseMapper.getCardinal(c);
            if (cardinal != null) {
                if (creator.inside() || BigDecimal.ONE.compareTo(cardinal) < 0) {
                    creator.attachCardinal(i, cardinal);
                }
                continue;
            }
            
            BigDecimal number = JapaneseMapper.getNumber(c);
            if (number != null) {
                creator.attachNumber(i, number);
                continue;
            }
            
            if (creator.inside()) {
                val.add(creator.value());
                creator = new NumberCreator();
            }
        }
        
        if (creator.inside()) {
            val.add(creator.value());
        }
        return val;
    }
}
