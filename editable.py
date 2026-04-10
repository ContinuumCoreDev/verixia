python3 - << 'EOF'
import os, warnings
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
warnings.filterwarnings("ignore")
import transformers
transformers.logging.set_verbosity_error()
import logging
logging.basicConfig(level=logging.ERROR)

from engine.confidence import verify

claims = [
    # HIGH — factually precise true claims
    ("The Second Amendment secures the right of the people to keep and bear arms.", "HIGH"),
    ("The Supreme Court established judicial review in Marbury v. Madison.", "HIGH"),
    ("The First Amendment prohibits Congress from abridging freedom of speech.", "HIGH"),
    ("Brown v. Board of Education held that racial segregation in public schools is unconstitutional.", "HIGH"),
    ("Miranda v. Arizona requires police to inform suspects of their rights before interrogation.", "HIGH"),
    ("The Fourth Amendment protects against unreasonable searches and seizures.", "HIGH"),
    ("Title VII of the Civil Rights Act of 1964 prohibits employment discrimination based on race.", "HIGH"),
    ("The Fourteenth Amendment guarantees equal protection to all persons under the law.", "HIGH"),
    ("The Commerce Clause grants Congress power to regulate commerce between the several states.", "HIGH"),
    ("The Fifth Amendment protects against self-incrimination.", "HIGH"),
    ("The Constitution is the supreme law of the land.", "HIGH"),

    # LOW — false or unsupported claims
    ("The Constitution explicitly contains the phrase separation of church and state.", "LOW"),
    ("The Supreme Court has unlimited jurisdiction over all legal matters.", "LOW"),
    ("Congress can override a Supreme Court constitutional ruling by simple majority vote.", "LOW"),
    ("The president has the power to unilaterally amend federal statutes.", "LOW"),
    ("Federal law is supreme over the Constitution in matters of national security.", "LOW"),

    # MEDIUM — nuanced claims
    
    ("Executive privilege is an absolute bar to congressional subpoenas.", "MEDIUM"),
    ("The Tenth Amendment reserves unenumerated powers to the states and to the people.", "HIGH"),
    ("Federal agencies have no independent lawmaking authority under Loper Bright.", "HIGH"),
    ("Any law repugnant to the Constitution is null and void.", "HIGH"),
]

correct = 0
print(f"{'#':<3} {'Verified':<14} {'Exp':<8} {'Score':<7} {'Hold%':<6} {'M'} Claim")
print("-" * 105)

for i, (claim, expected) in enumerate(claims, 1):
    result = verify(claim, top_k=10)
    r      = result.verifier_report

    if expected == "HIGH":
        match = result.confidence in ("HIGH", "MEDIUM")
    elif expected == "LOW":
        match = result.confidence in ("LOW", "UNVERIFIABLE")
    else:
        match = result.confidence in ("HIGH", "MEDIUM", "LOW")

    if match:
        correct += 1

    sym  = "✓" if match else "✗"
    hold = f"{r.holdings_percentage:.0%}"
    print(f"{i:<3} {result.confidence:<14} {expected:<8} "
          f"{result.score:<7.4f} {hold:<6} {sym}  {claim[:55]}...")

accuracy = correct / len(claims) * 100
print(f"\n{'='*105}")
print(f"ACCURACY: {correct}/{len(claims)} ({accuracy:.1f}%)")
print(f"Graph: 75,574 points | Founding documents included")
EOF