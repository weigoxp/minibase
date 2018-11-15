I have implemented *all* of the optional instructions:
  - ANDI
  - SLTI
  - BNE
  - LB
  - LUI
  - MUL
  - MULT
  - MFLO
  - MFHI
  - NOR
  - ORI
  - XORI
  - SB

How the extra 2 registers are used:
  32 - HI
  33 - LO

What the extra bits mean:
  ALU src (additional):
    2 - zero-extended imm16
    3 - constant ZERO

  ALU op (additional):
    5  - lui
    6  - shift left
    7  - shift right logical
    8  - shift right arithmetic
    9  - multiply
    10 - divide
    11 - nor

  regDst (choose destination register between alternatives):
    2 - HI/LO

  extra1, bit 0:
    Invert the 'zero' result  (used by BNE)
  extra2, bit 0:
    If 1, load/store bytes instead of words.
  extra3, all bits:
    ALU input1 src:
        0 - rsVal
        1 - shamt
        2 - HI register
        3 - LO register

